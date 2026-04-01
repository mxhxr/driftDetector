"""
================================================================
  api.py  —  DriftDetector Local API Bridge
  Flask server running on http://127.0.0.1:5050
  Started by open_ui_with_api.bat; consumed by drift_detector_ui.html
================================================================
"""
from __future__ import annotations
import json
import sys as _sys, os as _os
_sys.path.insert(0, _os.path.dirname(_os.path.abspath(__file__)))

# Load settings.env into os.environ before importing config.
_env_file = _os.path.join(_os.path.dirname(_os.path.abspath(__file__)), 'settings.env')
if _os.path.exists(_env_file):
    with open(_env_file, encoding='utf-8') as _f:
        for _line in _f:
            _line = _line.strip()
            if _line and not _line.startswith('#') and '=' in _line:
                _k, _, _v = _line.partition('=')
                _os.environ.setdefault(_k.strip(), _v.strip())

del _sys, _os, _env_file

import os
import socket
import subprocess
import threading
import time
import uuid
from datetime import datetime
from pathlib import Path

import pyodbc
from flask import Flask, Response, jsonify, request
from flask_cors import CORS

# ── Bootstrap ──────────────────────────────────────────────────────────────
ROOT     = Path(__file__).parent.resolve()
DATA_DIR = ROOT / "data"
DATA_DIR.mkdir(exist_ok=True)

SERVERS_FILE       = DATA_DIR / "servers.json"
IMPORT_CONFIG_FILE = DATA_DIR / "import_config.json"
ENV_FILE           = ROOT / "settings.env"

app = Flask(__name__)
CORS(app, resources={r"/api/*": {"origins": "*"}})

# In-memory audit run tracker  {run_id: {status, logs[], progress, exitCode}}
_audit_runs: dict[str, dict] = {}


# ══════════════════════════════════════════════════════════════════════════════
#  Helpers
# ══════════════════════════════════════════════════════════════════════════════

def _read_env() -> dict[str, str]:
    """Parse settings.env into a plain dict (skips comments/blanks)."""
    settings: dict[str, str] = {}
    if ENV_FILE.exists():
        for line in ENV_FILE.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, _, v = line.partition("=")
                settings[k.strip()] = v.strip()
    return settings


def _write_env(new_values: dict[str, str]) -> None:
    """Write key=value pairs into settings.env, preserving comment lines."""
    lines: list[str] = []
    handled: set[str] = set()

    if ENV_FILE.exists():
        for line in ENV_FILE.read_text(encoding="utf-8").splitlines():
            stripped = line.strip()
            if not stripped or stripped.startswith("#"):
                lines.append(line)
                continue
            if "=" in stripped:
                k = stripped.split("=", 1)[0].strip()
                if k in new_values:
                    lines.append(f"{k}={new_values[k]}")
                    handled.add(k)
                    continue
            lines.append(line)

    for k, v in new_values.items():
        if k not in handled:
            lines.append(f"{k}={v}")

    ENV_FILE.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _conn_str(server: str, database: str = "master", trusted: bool = True,
              user: str = "", pwd: str = "",
              driver: str = "ODBC Driver 17 for SQL Server",
              timeout: int = 30) -> str:
    base = (f"DRIVER={{{driver}}};SERVER={server};DATABASE={database};"
            f"Connect Timeout={timeout};APP=DriftDetector;")
    return base + ("Trusted_Connection=yes;" if trusted else f"UID={user};PWD={pwd};")


def _repo_conn_str(s: dict[str, str] | None = None) -> str:
    s = s or _read_env()
    return _conn_str(
        server=s.get("DRIFT_REPO_SERVER", ""),
        database=s.get("DRIFT_REPO_DB", "DriftDetector"),
        trusted=s.get("DRIFT_TRUSTED", "1") == "1",
        user=s.get("DRIFT_SQL_USER", ""),
        pwd=s.get("DRIFT_SQL_PWD", ""),
        driver=s.get("DRIFT_ODBC_DRIVER", "ODBC Driver 17 for SQL Server"),
        timeout=int(s.get("DRIFT_CONN_TIMEOUT", "30")),
    )


def _wizard_conn_str(d: dict) -> str:
    return _conn_str(
        server=d["server"],
        database=d.get("database", "master"),
        trusted=d.get("trusted", True),
        user=d.get("user", ""),
        pwd=d.get("pwd", ""),
        driver=d.get("driver", "ODBC Driver 17 for SQL Server"),
        timeout=15,
    )


def _load_servers() -> list[dict]:
    if SERVERS_FILE.exists():
        return json.loads(SERVERS_FILE.read_text(encoding="utf-8"))
    return []


def _save_servers(servers: list[dict]) -> None:
    SERVERS_FILE.write_text(json.dumps(servers, indent=2), encoding="utf-8")


# ══════════════════════════════════════════════════════════════════════════════
#  Health
# ══════════════════════════════════════════════════════════════════════════════

@app.route("/api/health")
def health():
    return jsonify({"ok": True, "version": "1.0.0",
                    "ts": datetime.utcnow().isoformat()})


# ══════════════════════════════════════════════════════════════════════════════
#  Settings  (reads / writes settings.env)
# ══════════════════════════════════════════════════════════════════════════════

@app.route("/api/settings", methods=["GET"])
def get_settings():
    return jsonify(_read_env())


@app.route("/api/settings", methods=["POST"])
def save_settings():
    data: dict = request.json or {}
    _write_env(data)
    # Reload config module so live run picks up changes immediately
    try:
        import importlib, config as cfg
        importlib.reload(cfg)
    except Exception:
        pass
    return jsonify({"ok": True})


# ══════════════════════════════════════════════════════════════════════════════
#  Connection testing
# ══════════════════════════════════════════════════════════════════════════════

@app.route("/api/test-connection", methods=["POST"])
def test_connection():
    d: dict = request.json or {}
    try:
        cs = _wizard_conn_str({**d, "database": "master"})
        t0 = time.time()
        conn = pyodbc.connect(cs, autocommit=True, timeout=10)
        ms = int((time.time() - t0) * 1000)
        # Grab SQL Server version while we're here
        ver = conn.cursor().execute("SELECT @@VERSION").fetchone()[0].split("\n")[0]
        conn.close()
        return jsonify({"ok": True, "ms": ms, "version": ver})
    except pyodbc.Error as exc:
        return jsonify({"ok": False, "error": str(exc)}), 400


# ══════════════════════════════════════════════════════════════════════════════
#  Schema discovery  (databases → tables → columns)
# ══════════════════════════════════════════════════════════════════════════════

@app.route("/api/databases", methods=["POST"])
def list_databases():
    d: dict = request.json or {}
    try:
        cs = _wizard_conn_str({**d, "database": "master"})
        conn = pyodbc.connect(cs, autocommit=True, timeout=15)
        cur = conn.cursor()
        cur.execute("""
            SELECT name FROM sys.databases
            WHERE  state_desc = 'ONLINE'
              AND  name NOT IN ('master','model','msdb','tempdb','distribution')
            ORDER  BY name
        """)
        dbs = [r.name for r in cur.fetchall()]
        conn.close()
        return jsonify({"databases": dbs})
    except pyodbc.Error as exc:
        return jsonify({"error": str(exc)}), 400


@app.route("/api/tables", methods=["POST"])
def list_tables():
    d: dict = request.json or {}
    try:
        cs = _wizard_conn_str(d)
        conn = pyodbc.connect(cs, autocommit=True, timeout=20)
        cur = conn.cursor()
        cur.execute("""
            SELECT t.TABLE_SCHEMA, t.TABLE_NAME, t.TABLE_TYPE,
                   (SELECT COUNT(*)
                    FROM   INFORMATION_SCHEMA.COLUMNS c
                    WHERE  c.TABLE_SCHEMA = t.TABLE_SCHEMA
                      AND  c.TABLE_NAME   = t.TABLE_NAME) AS col_count
            FROM   INFORMATION_SCHEMA.TABLES t
            ORDER  BY t.TABLE_TYPE, t.TABLE_SCHEMA, t.TABLE_NAME
        """)
        rows = cur.fetchall()
        tables = []
        for row in rows:
            row_count = None
            if row.TABLE_TYPE == "BASE TABLE":
                try:
                    rc = conn.cursor()
                    rc.execute(
                        f"SELECT SUM(p.rows) FROM sys.partitions p "
                        f"JOIN sys.tables tbl ON tbl.object_id=p.object_id "
                        f"JOIN sys.schemas s ON s.schema_id=tbl.schema_id "
                        f"WHERE s.name=? AND tbl.name=? AND p.index_id IN (0,1)",
                        row.TABLE_SCHEMA, row.TABLE_NAME,
                    )
                    rc_row = rc.fetchone()
                    row_count = int(rc_row[0]) if rc_row and rc_row[0] else 0
                except Exception:
                    pass
            tables.append({
                "schema":  row.TABLE_SCHEMA,
                "name":    row.TABLE_NAME,
                "type":    "table" if row.TABLE_TYPE == "BASE TABLE" else "view",
                "cols":    row.col_count,
                "rows":    row_count,
            })
        conn.close()
        return jsonify({"tables": tables})
    except pyodbc.Error as exc:
        return jsonify({"error": str(exc)}), 400


@app.route("/api/columns", methods=["POST"])
def list_columns():
    d: dict = request.json or {}
    tbl = d.get("table", "")
    schema, name = tbl.split(".", 1) if "." in tbl else ("dbo", tbl)
    try:
        cs = _wizard_conn_str(d)
        conn = pyodbc.connect(cs, autocommit=True, timeout=15)
        cur = conn.cursor()
        cur.execute("""
            SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE
            FROM   INFORMATION_SCHEMA.COLUMNS
            WHERE  TABLE_SCHEMA = ? AND TABLE_NAME = ?
            ORDER  BY ORDINAL_POSITION
        """, schema, name)
        cols = [{"name": r.COLUMN_NAME, "type": r.DATA_TYPE,
                 "nullable": r.IS_NULLABLE == "YES"}
                for r in cur.fetchall()]
        conn.close()
        return jsonify({"columns": cols})
    except pyodbc.Error as exc:
        return jsonify({"error": str(exc)}), 400


# ══════════════════════════════════════════════════════════════════════════════
#  Import servers from SQL
# ══════════════════════════════════════════════════════════════════════════════

@app.route("/api/import-servers", methods=["POST"])
def import_servers():
    d: dict = request.json or {}
    tbl        = d.get("table", "")
    col_server = d["colServer"]
    col_env    = d.get("colEnv",  "")
    col_desc   = d.get("colDesc", "")
    where      = d.get("where",   "").strip()
    preview_only = d.get("previewOnly", False)

    schema, name = tbl.split(".", 1) if "." in tbl else ("dbo", tbl)
    sel_cols = [f"[{col_server}]"]
    if col_env:  sel_cols.append(f"[{col_env}]")
    if col_desc: sel_cols.append(f"[{col_desc}]")
    sql = f"SELECT TOP 500 {', '.join(sel_cols)} FROM [{schema}].[{name}]"
    if where:
        sql += f" WHERE {where}"

    try:
        cs = _wizard_conn_str(d)
        conn = pyodbc.connect(cs, autocommit=True, timeout=30)
        cur  = conn.cursor()
        cur.execute(sql)
        source = (f"{d['server']} → [{d.get('database','?')}]."
                  f"{tbl}.{col_server}")
        rows = []
        for row in cur.fetchall():
            srv_name = str(row[0]).strip() if row[0] else None
            if not srv_name:
                continue
            entry = {
                "name":      srv_name,
                "env":       str(row[sel_cols.index(f"[{col_env}]")]).strip()
                             if col_env and f"[{col_env}]" in sel_cols else "Unknown",
                "desc":      str(row[sel_cols.index(f"[{col_desc}]")]).strip()
                             if col_desc and f"[{col_desc}]" in sel_cols else "",
                "status":    "unknown",
                "lastAudit": "Never",
                "drifts":    0,
                "source":    source,
            }
            rows.append(entry)
        conn.close()

        if not preview_only:
            if d.get("replace"):
                existing = []
            else:
                existing = _load_servers()

            existing_names = {s["name"].lower() for s in existing}
            added = 0
            for r in rows:
                if r["name"].lower() not in existing_names:
                    existing.append(r)
                    added += 1
            _save_servers(existing)

            if d.get("saveConfig"):
                IMPORT_CONFIG_FILE.write_text(json.dumps({
                    "server":   d["server"],
                    "database": d.get("database", ""),
                    "table":    tbl,
                    "colServer": col_server,
                    "colEnv":   col_env,
                    "colDesc":  col_desc,
                    "where":    where,
                    "trusted":  d.get("trusted", True),
                    "user":     d.get("user", ""),
                    "driver":   d.get("driver", "ODBC Driver 17 for SQL Server"),
                }, indent=2), encoding="utf-8")

        return jsonify({"rows": rows, "sql": sql, "count": len(rows)})
    except pyodbc.Error as exc:
        return jsonify({"error": str(exc)}), 400


@app.route("/api/import-config", methods=["GET"])
def get_import_config():
    if IMPORT_CONFIG_FILE.exists():
        return jsonify(json.loads(IMPORT_CONFIG_FILE.read_text(encoding="utf-8")))
    return jsonify(None)


@app.route("/api/import-config", methods=["DELETE"])
def delete_import_config():
    if IMPORT_CONFIG_FILE.exists():
        IMPORT_CONFIG_FILE.unlink()
    return jsonify({"ok": True})


# ══════════════════════════════════════════════════════════════════════════════
#  Server list CRUD
# ══════════════════════════════════════════════════════════════════════════════

@app.route("/api/servers", methods=["GET"])
def get_servers():
    return jsonify(_load_servers())


@app.route("/api/servers", methods=["POST"])
def add_server():
    d: dict = request.json or {}
    servers = _load_servers()
    if any(s["name"].lower() == d["name"].lower() for s in servers):
        return jsonify({"error": "Server already exists"}), 409
    entry = {
        "name":      d["name"],
        "env":       d.get("env", "Unknown"),
        "desc":      d.get("desc", "Manually added"),
        "status":    "unknown",
        "lastAudit": "Never",
        "drifts":    0,
        "source":    None,
    }
    servers.append(entry)
    _save_servers(servers)
    return jsonify(entry), 201


@app.route("/api/servers", methods=["PUT"])
def replace_servers():
    servers = request.json or []
    _save_servers(servers)
    return jsonify({"ok": True, "count": len(servers)})


@app.route("/api/servers/<path:name>", methods=["DELETE"])
def delete_server(name: str):
    servers = [s for s in _load_servers() if s["name"] != name]
    _save_servers(servers)
    return jsonify({"ok": True})


@app.route("/api/servers/<path:name>/ping", methods=["POST"])
def ping_server(name: str):
    host = name.split("\\")[0].split(",")[0]  # strip instance + port
    port = 1433
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(3)
        result = sock.connect_ex((host, port))
        sock.close()
        status = "online" if result == 0 else "offline"
    except Exception:
        status = "offline"

    servers = _load_servers()
    for s in servers:
        if s["name"] == name:
            s["status"] = status
            break
    _save_servers(servers)
    return jsonify({"status": status})


# ══════════════════════════════════════════════════════════════════════════════
#  Dashboard stats  (from Fact_Audit_History)
# ══════════════════════════════════════════════════════════════════════════════

@app.route("/api/dashboard/stats")
def dashboard_stats():
    try:
        conn = pyodbc.connect(_repo_conn_str(), autocommit=True, timeout=20)
        cur  = conn.cursor()

        # Latest run ID
        cur.execute("""
            SELECT TOP 1 RunID, AuditTimestamp
            FROM   dbo.Fact_Audit_History
            ORDER  BY AuditTimestamp DESC
        """)
        row = cur.fetchone()
        if not row:
            conn.close()
            return jsonify({"hasData": False})

        run_id = str(row.RunID)
        latest_ts = str(row.AuditTimestamp)[:19]

        # Status counts for latest run
        cur.execute("""
            SELECT Status, COUNT(*) AS cnt
            FROM   dbo.Fact_Audit_History
            WHERE  RunID = ?
            GROUP  BY Status
        """, run_id)
        counts = {r.Status: r.cnt for r in cur.fetchall()}

        # Open drift items
        cur.execute("""
            SELECT TOP 100
                   ServerName, DatabaseName, CheckCategory, CheckItem,
                   CurrentValue, TargetValue, Status
            FROM   dbo.Fact_Audit_History
            WHERE  RunID = ? AND Status = 'DRIFT'
            ORDER  BY ServerName, CheckCategory
        """, run_id)
        drifts = [{"server": r.ServerName, "db": r.DatabaseName,
                   "cat": r.CheckCategory, "item": r.CheckItem,
                   "cur": r.CurrentValue, "tgt": r.TargetValue,
                   "status": r.Status}
                  for r in cur.fetchall()]

        # Trend: drift count per run, last 10 runs
        cur.execute("""
            SELECT   RunID,
                     CAST(MIN(AuditTimestamp) AS DATE) AS RunDate,
                     SUM(CASE WHEN Status='DRIFT' THEN 1 ELSE 0 END) AS DriftCount
            FROM     dbo.Fact_Audit_History
            GROUP BY RunID
            ORDER BY MIN(AuditTimestamp) DESC
            OFFSET   0 ROWS FETCH NEXT 10 ROWS ONLY
        """)
        trend = [{"date": str(r.RunDate), "drifts": r.DriftCount}
                 for r in cur.fetchall()]
        trend.reverse()

        # Category breakdown for latest run drifts
        cur.execute("""
            SELECT CheckCategory, COUNT(*) AS cnt
            FROM   dbo.Fact_Audit_History
            WHERE  RunID = ? AND Status = 'DRIFT'
            GROUP  BY CheckCategory
        """, run_id)
        by_cat = {r.CheckCategory: r.cnt for r in cur.fetchall()}

        conn.close()
        return jsonify({
            "hasData":    True,
            "runId":      run_id,
            "latestTs":   latest_ts,
            "total":      sum(counts.values()),
            "pass":       counts.get("PASS",    0),
            "drift":      counts.get("DRIFT",   0),
            "error":      counts.get("ERROR",   0),
            "skipped":    counts.get("SKIPPED", 0),
            "driftItems": drifts,
            "trend":      trend,
            "byCategory": by_cat,
        })
    except Exception as exc:
        return jsonify({"hasData": False, "error": str(exc)}), 500


# ══════════════════════════════════════════════════════════════════════════════
#  Audit results browser
# ══════════════════════════════════════════════════════════════════════════════

@app.route("/api/results")
def get_results():
    srv_f    = request.args.get("server",   "")
    stat_f   = request.args.get("status",   "")
    cat_f    = request.args.get("category", "")
    search_f = request.args.get("search",   "")
    limit    = min(int(request.args.get("limit", "500")), 2000)

    try:
        conn = pyodbc.connect(_repo_conn_str(), autocommit=True, timeout=20)
        cur  = conn.cursor()

        sql    = f"SELECT TOP ({limit}) AuditTimestamp, ServerName, DatabaseName, CheckCategory, CheckItem, CurrentValue, TargetValue, Status FROM dbo.Fact_Audit_History WHERE 1=1"
        params: list = []
        if srv_f:    sql += " AND ServerName = ?";      params.append(srv_f)
        if stat_f:   sql += " AND Status = ?";          params.append(stat_f)
        if cat_f:    sql += " AND CheckCategory = ?";   params.append(cat_f)
        if search_f: sql += " AND CheckItem LIKE ?";    params.append(f"%{search_f}%")
        sql += " ORDER BY AuditTimestamp DESC"

        cur.execute(sql, params)
        rows = [{"ts": str(r.AuditTimestamp)[:19], "server": r.ServerName,
                 "db": r.DatabaseName, "cat": r.CheckCategory,
                 "item": r.CheckItem, "cur": r.CurrentValue,
                 "tgt": r.TargetValue, "status": r.Status}
                for r in cur.fetchall()]

        cur.execute("SELECT DISTINCT ServerName FROM dbo.Fact_Audit_History ORDER BY ServerName")
        servers = [r.ServerName for r in cur.fetchall()]

        conn.close()
        return jsonify({"rows": rows, "servers": servers, "total": len(rows)})
    except Exception as exc:
        return jsonify({"rows": [], "servers": [], "error": str(exc), "total": 0}), 500


# ══════════════════════════════════════════════════════════════════════════════
#  CIS Baselines
# ══════════════════════════════════════════════════════════════════════════════

@app.route("/api/baselines")
def get_baselines():
    try:
        conn = pyodbc.connect(_repo_conn_str(), autocommit=True, timeout=15)
        cur  = conn.cursor()
        cur.execute("""
            SELECT CheckCategory, CheckItem, TargetValue,
                   ComparisonOperator, CISControl, IsActive
            FROM   dbo.CIS_Baselines
            ORDER  BY CheckCategory, CheckItem
        """)
        bl = [{"cat": r.CheckCategory, "item": r.CheckItem,
               "target": r.TargetValue, "op": r.ComparisonOperator,
               "ctrl": r.CISControl, "active": bool(r.IsActive)}
              for r in cur.fetchall()]
        conn.close()
        return jsonify({"baselines": bl})
    except Exception as exc:
        return jsonify({"baselines": [], "error": str(exc)}), 500


# ══════════════════════════════════════════════════════════════════════════════
#  Reports
# ══════════════════════════════════════════════════════════════════════════════

@app.route("/api/reports")
def list_reports():
    reports_dir = ROOT / "reports"
    files = []
    if reports_dir.exists():
        for f in sorted(reports_dir.glob("DriftReport_*.xlsx"), reverse=True):
            stat     = f.stat()
            size_kb  = max(1, stat.st_size // 1024)
            modified = datetime.fromtimestamp(stat.st_mtime).strftime("%Y-%m-%d %H:%M:%S")
            files.append({
                "filename": f.name,
                "modified": modified,
                "size":     f"{size_kb} KB",
            })
    return jsonify({"reports": files})


@app.route("/api/reports/open/<filename>", methods=["POST"])
def open_report(filename: str):
    path = ROOT / "reports" / filename
    if not path.exists() or path.suffix != ".xlsx":
        return jsonify({"error": "File not found"}), 404
    try:
        os.startfile(str(path))          # Windows shell open
    except AttributeError:
        subprocess.Popen(["xdg-open", str(path)])   # Linux fallback
    return jsonify({"ok": True})


@app.route("/api/reports/<filename>", methods=["DELETE"])
def delete_report(filename: str):
    path = ROOT / "reports" / filename
    if path.exists():
        path.unlink()
        return jsonify({"ok": True})
    return jsonify({"error": "File not found"}), 404


# ══════════════════════════════════════════════════════════════════════════════
#  Audit execution  (spawns main.py subprocess, streams output via SSE)
# ══════════════════════════════════════════════════════════════════════════════

@app.route("/api/audit/run", methods=["POST"])
def start_audit():
    d       = request.json or {}
    run_id  = str(uuid.uuid4())
    dry_run = d.get("dryRun",   False)
    ff      = d.get("failFast", False)
    servers = d.get("servers",  [])

    _audit_runs[run_id] = {
        "status":   "running",
        "logs":     [],
        "progress": 0,
        "exitCode": None,
    }

    def _run() -> None:
        cmd = ["python", str(ROOT / "main.py")]
        if dry_run:  cmd.append("--dry-run")
        if ff:       cmd.append("--fail-fast")
        if servers:  cmd += ["--servers"] + servers

        try:
            proc = subprocess.Popen(
                cmd,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                cwd=str(ROOT),
                bufsize=1,
            )
            total_lines = 0
            for raw_line in proc.stdout:
                line = raw_line.rstrip()
                if line:
                    _audit_runs[run_id]["logs"].append(line)
                    total_lines += 1
                    # Estimate progress from log markers
                    if "Auditing" in line and "server" in line:
                        _audit_runs[run_id]["progress"] = min(
                            _audit_runs[run_id]["progress"] + 15, 85)
                    elif "complete" in line.lower() or "Run complete" in line:
                        _audit_runs[run_id]["progress"] = 95
            proc.wait()
            _audit_runs[run_id]["exitCode"]  = proc.returncode
            _audit_runs[run_id]["status"]    = "complete"
            _audit_runs[run_id]["progress"]  = 100
        except Exception as exc:
            _audit_runs[run_id]["logs"].append(f"FATAL: {exc}")
            _audit_runs[run_id]["status"]   = "error"
            _audit_runs[run_id]["progress"] = 100

    threading.Thread(target=_run, daemon=True).start()
    return jsonify({"runId": run_id})


@app.route("/api/audit/stream/<run_id>")
def stream_audit(run_id: str):
    """Server-Sent Events endpoint — streams log lines as they arrive."""
    def _generate():
        cursor = 0
        while True:
            run = _audit_runs.get(run_id)
            if not run:
                yield f"data: {json.dumps({'error': 'Run not found'})}\n\n"
                return

            logs = run["logs"]
            if cursor < len(logs):
                for line in logs[cursor:]:
                    payload = json.dumps({
                        "line":     line,
                        "progress": run["progress"],
                        "status":   run["status"],
                    })
                    yield f"data: {payload}\n\n"
                cursor = len(logs)

            if run["status"] in ("complete", "error"):
                yield f"data: {json.dumps({'done': True, 'exitCode': run['exitCode'], 'status': run['status']})}\n\n"
                return

            time.sleep(0.15)

    return Response(
        _generate(),
        mimetype="text/event-stream",
        headers={
            "Cache-Control":   "no-cache",
            "X-Accel-Buffering": "no",
            "Connection":      "keep-alive",
        },
    )


# ══════════════════════════════════════════════════════════════════════════════
#  Entry point
# ══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    print("=" * 60)
    print("  DriftDetector API  —  http://127.0.0.1:5050")
    print("  Keep this window open while using the UI.")
    print("  Close it (or Ctrl+C) to stop the server.")
    print("=" * 60)
    app.run(host="127.0.0.1", port=5050, debug=False, threaded=True)
