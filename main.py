"""
=============================================================
  main.py  –  Configuration Drift Detection Orchestrator
  SQL Server CIS Benchmark Compliance-as-Code
=============================================================

Usage
-----
  python main.py                    # Normal run
  python main.py --dry-run          # Audit only; no DB writes, report always saved
  python main.py --servers SRV1 SRV2  # Override server list
  python main.py --fail-fast        # Stop on first connection error

Windows Task Scheduler example action:
  Program : C:\\Python312\\python.exe
  Arguments: C:\\DriftDetector\\main.py
  Start in : C:\\DriftDetector
"""
from __future__ import annotations

# Ensure the directory containing this script is on sys.path.
# Required when running with the embedded Python where the script
# directory is not added automatically.
import sys as _sys
import os as _os
_sys.path.insert(0, _os.path.dirname(_os.path.abspath(__file__)))

# Load settings.env into os.environ BEFORE importing config.
# config.py reads all settings via os.getenv() so this must happen first.
_env_file = _os.path.join(_os.path.dirname(_os.path.abspath(__file__)), 'settings.env')
if _os.path.exists(_env_file):
    with open(_env_file, encoding='utf-8') as _f:
        for _line in _f:
            _line = _line.strip()
            if _line and not _line.startswith('#') and '=' in _line:
                _k, _, _v = _line.partition('=')
                _os.environ.setdefault(_k.strip(), _v.strip())

del _sys, _os, _env_file

import argparse
import logging
import sys
import uuid
from datetime import datetime, timezone

import pyodbc

import config
from audit_checks import audit_server
from db_manager import (
    AuditResult,
    get_audit_conn,
    load_baselines,
    persist_results,
)
from drift_detector import (
    extract_drifts,
    per_server_summary,
    results_to_dataframe,
    summarise_run,
)
from reporter import generate_report


# -- Logging setup ----------------------------------------------------------

def _setup_logging(level: str) -> None:
    fmt = "%(asctime)s [%(levelname)-8s] %(name)s – %(message)s"
    date_fmt = "%Y-%m-%d %H:%M:%S"
    numeric_level = getattr(logging, level.upper(), logging.INFO)
    log_file = config.LOGS_DIR / f"drift_run_{datetime.now().strftime('%Y%m%d')}.log"

    # Force UTF-8 on stdout so Unicode characters don't crash on cp1252 consoles
    import io
    utf8_stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace', line_buffering=True)
    stream_handler = logging.StreamHandler(utf8_stdout)
    stream_handler.setFormatter(logging.Formatter(fmt=fmt, datefmt=date_fmt))

    file_handler = logging.FileHandler(log_file, encoding='utf-8')
    file_handler.setFormatter(logging.Formatter(fmt=fmt, datefmt=date_fmt))

    root = logging.getLogger()
    root.setLevel(numeric_level)
    root.handlers.clear()
    root.addHandler(stream_handler)
    root.addHandler(file_handler)


logger = logging.getLogger("main")


# -- CLI argument parsing ---------------------------------------------------

def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="SQL Server Configuration Drift Detector (CIS Benchmark)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        default=False,
        help="Run all audits but skip DB writes. Report is always generated.",
    )
    parser.add_argument(
        "--fail-fast",
        action="store_true",
        default=False,
        help="Abort the entire run on the first server connection error.",
    )
    parser.add_argument(
        "--servers",
        nargs="+",
        metavar="SERVER",
        help="Override the server list from config.py.",
    )
    parser.add_argument(
        "--log-level",
        default=config.LOG_LEVEL,
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging verbosity (default: %(default)s).",
    )
    return parser.parse_args()


# -- Banner -----------------------------------------------------------------

def _print_banner(run_id: str, dry_run: bool) -> None:
    width = 66
    logger.info("=" * width)
    logger.info("  SQL Server Configuration Drift Detection")
    logger.info("  CIS Benchmark Compliance-as-Code")
    logger.info("-" * width)
    logger.info("  Run ID   : %s", run_id)
    logger.info("  Started  : %s UTC", datetime.now(tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S"))
    if dry_run:
        logger.info("  *** DRY-RUN MODE – no writes to repository DB ***")
    logger.info("=" * width)


# -- Single-server audit wrapper --------------------------------------------

def _audit_one_server(
    server_cfg: dict,
    run_id: uuid.UUID,
) -> list[AuditResult]:
    """
    Connects to `server_cfg["server"]`, runs all checks, and returns results.
    Returns a single ERROR result if the connection fails.
    """
    server = server_cfg["server"]
    logger.info("-- Auditing server: %s", server)
    try:
        with get_audit_conn(server) as conn:
            return audit_server(conn, server, run_id)
    except pyodbc.Error as exc:
        logger.error("  ✗ Cannot connect to [%s]: %s", server, exc)
        return [
            AuditResult(
                run_id=run_id,
                server_name=server,
                database_name=None,
                check_category="CONNECTION",
                check_item="connectivity",
                current_value=None,
                target_value=None,
                status="ERROR",
                error_message=str(exc),
            )
        ]


# -- Main -------------------------------------------------------------------

def main() -> int:
    args    = _parse_args()
    dry_run = args.dry_run or config.DRY_RUN
    fail_fast = args.fail_fast or config.FAIL_FAST

    # Apply CLI overrides to module-level config flags
    config.DRY_RUN   = dry_run
    config.FAIL_FAST = fail_fast

    _setup_logging(args.log_level)

    run_id  = uuid.uuid4()
    run_str = str(run_id)
    _print_banner(run_str, dry_run)

    # -- 1. Load baselines -------------------------------------------------
    try:
        baselines = load_baselines()
    except Exception as exc:
        logger.critical("Cannot load baselines from repository: %s", exc)
        return 1

    if not baselines:
        logger.critical("No active baselines found in CIS_Baselines table. Aborting.")
        return 1

    # -- 2. Build server list ----------------------------------------------
    servers: list[dict] = (
        [{"server": s} for s in args.servers]
        if args.servers
        else config.TARGET_SERVERS
    )

    if not servers:
        logger.error("No target servers configured. Add entries to config.TARGET_SERVERS.")
        return 1

    logger.info("Auditing %d server(s) ...", len(servers))

    # -- 3. Audit each server ----------------------------------------------
    all_results: list[AuditResult] = []
    connection_errors: list[str]   = []

    for srv in servers:
        results = _audit_one_server(srv, run_id)
        all_results.extend(results)

        # Track connection-level errors for fail-fast check
        conn_errors = [r for r in results if r.check_category == "CONNECTION"]
        if conn_errors:
            connection_errors.append(srv["server"])
            if fail_fast:
                logger.warning("--fail-fast: aborting after connection error on [%s].", srv["server"])
                break

    # -- 4. Build DataFrames -----------------------------------------------
    full_df    = results_to_dataframe(all_results, baselines)
    drift_df   = extract_drifts(full_df)
    summary_df = per_server_summary(full_df)
    run_stats  = summarise_run(full_df)

    # -- 5. Log summary ----------------------------------------------------
    logger.info("-" * 66)
    logger.info(
        "Run summary -> Total: %d | PASS: %d | DRIFT: %d | ERROR: %d | SKIPPED: %d",
        run_stats["total"],
        run_stats["pass"],
        run_stats["drift"],
        run_stats["error"],
        run_stats["skipped"],
    )

    if not drift_df.empty:
        logger.warning("Drifted items by server:")
        for srv, grp in drift_df.groupby("server_name"):
            logger.warning("  [%s] -> %d drift(s)", srv, len(grp))

    # -- 6. Persist to repository ------------------------------------------
    try:
        persist_results(all_results, dry_run=dry_run)
    except Exception as exc:
        logger.error("Failed to persist results to repository: %s", exc)
        # Non-fatal – still generate the report

    # -- 7. Generate Excel report ------------------------------------------
    try:
        report_path = generate_report(
            drift_df   = drift_df,
            summary_df = summary_df,
            run_summary= run_stats,
            run_id     = run_str,
        )
        if report_path:
            logger.info("Report -> %s", report_path)
        else:
            logger.info("All checks PASSED – no Excel report generated.")
    except Exception as exc:
        logger.error("Failed to generate Excel report: %s", exc)

    logger.info("=" * 66)
    logger.info("Drift detection run complete.")

    # Return non-zero exit code if any drifts or errors found
    # (useful for Task Scheduler "Last Run Result" monitoring)
    return 0 if (run_stats["drift"] == 0 and run_stats["error"] == 0) else 2


if __name__ == "__main__":
    sys.exit(main())
