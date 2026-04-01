# =============================================================
#  config.py  —  DriftDetector configuration
#  All paths are relative to this file's directory so the
#  package runs from any folder without editing source code.
#  Override any value with an environment variable (see below).
# =============================================================
from __future__ import annotations
import os
from pathlib import Path

# Root of the package — always the folder this file lives in
ROOT = Path(__file__).parent.resolve()

# Load settings.env directly into os.environ so all os.getenv() calls below
# work correctly regardless of how this module was imported or from where.
# Using setdefault means real environment variables always take priority.
_env_path = ROOT / "settings.env"
if _env_path.exists():
    for _line in _env_path.read_text(encoding="utf-8").splitlines():
        _line = _line.strip()
        if _line and not _line.startswith("#") and "=" in _line:
            _k, _, _v = _line.partition("=")
            os.environ.setdefault(_k.strip(), _v.strip())
del _env_path

# ── Target servers to audit ────────────────────────────────────────────────
# Populated by the UI wizard or edited directly here.
# Each entry: {"server": "<hostname or hostname\\instance>"}
TARGET_SERVERS: list[dict] = [
    # {"server": "SQLPROD01"},
    # {"server": "SQLPROD02\\INST1"},
]

# ── Central repository DB ──────────────────────────────────────────────────
REPO_DB = {
    "server":   os.getenv("DRIFT_REPO_SERVER", ""),          # e.g. SQLPROD01
    "database": os.getenv("DRIFT_REPO_DB",     "DriftDetector"),
    "trusted":  os.getenv("DRIFT_TRUSTED",     "1") == "1",  # Windows Auth
    "sql_user": os.getenv("DRIFT_SQL_USER",    ""),
    "sql_pwd":  os.getenv("DRIFT_SQL_PWD",     ""),
    "driver":   os.getenv("DRIFT_ODBC_DRIVER", "ODBC Driver 17 for SQL Server"),
    "timeout":  int(os.getenv("DRIFT_CONN_TIMEOUT", "30")),
}

# ── Audit target connection defaults ──────────────────────────────────────
AUDIT_CONN = {
    "trusted":  os.getenv("DRIFT_TRUSTED",      "1") == "1",
    "sql_user": os.getenv("DRIFT_AUDIT_USER",   ""),
    "sql_pwd":  os.getenv("DRIFT_AUDIT_PWD",    ""),
    "driver":   os.getenv("DRIFT_ODBC_DRIVER",  "ODBC Driver 17 for SQL Server"),
    "timeout":  int(os.getenv("DRIFT_CONN_TIMEOUT", "30")),
}

# ── Databases excluded from guest / orphan checks ─────────────────────────
SYSTEM_DATABASES: set[str] = {"master", "msdb", "tempdb", "model", "distribution"}

# ── Output — relative to package root ─────────────────────────────────────
REPORTS_DIR = ROOT / os.getenv("DRIFT_REPORTS_DIR", "reports")
LOGS_DIR    = ROOT / "logs"
REPORTS_DIR.mkdir(parents=True, exist_ok=True)
LOGS_DIR.mkdir(parents=True,    exist_ok=True)

LOG_LEVEL = os.getenv("DRIFT_LOG_LEVEL", "INFO")

# ── Runtime flags ─────────────────────────────────────────────────────────
DRY_RUN:   bool = False
FAIL_FAST: bool = False
