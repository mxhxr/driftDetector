
# =============================================================
#  diagnose.py  --  DriftDetector Connection Diagnostics
#  Run from the DriftDetector folder:
#    python\python.exe diagnose.py
# =============================================================
import sys
import os
import socket

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

ROOT = os.path.dirname(os.path.abspath(__file__))

def sep(): print("-" * 60)
def ok(t):   print(f"  [OK]   {t}")
def fail(t): print(f"  [FAIL] {t}")
def info(t): print(f"  [--]   {t}")
def warn(t): print(f"  [!!]   {t}")

print()
print("=" * 60)
print("  DriftDetector -- Connection Diagnostics")
print("=" * 60)

# ── 1. Read settings.env ───────────────────────────────────────
sep()
print("  Step 1: Reading settings.env")
sep()

env_path = os.path.join(ROOT, "settings.env")
settings = {}
if os.path.exists(env_path):
    for line in open(env_path, encoding="utf-8"):
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, _, v = line.partition("=")
            settings[k.strip()] = v.strip()
    ok(f"Loaded {env_path}")
else:
    fail("settings.env not found")
    sys.exit(1)

repo_server  = settings.get("DRIFT_REPO_SERVER", "")
repo_db      = settings.get("DRIFT_REPO_DB", "DriftDetector")
trusted      = settings.get("DRIFT_TRUSTED", "1") == "1"
sql_user     = settings.get("DRIFT_SQL_USER", "")
sql_pwd      = settings.get("DRIFT_SQL_PWD", "")
driver       = settings.get("DRIFT_ODBC_DRIVER", "ODBC Driver 17 for SQL Server")
timeout      = settings.get("DRIFT_CONN_TIMEOUT", "30")

print()
info(f"DRIFT_REPO_SERVER  = '{repo_server}'")
info(f"DRIFT_REPO_DB      = '{repo_db}'")
info(f"DRIFT_TRUSTED      = '{settings.get('DRIFT_TRUSTED','1')}'")
info(f"DRIFT_ODBC_DRIVER  = '{driver}'")
info(f"DRIFT_CONN_TIMEOUT = '{timeout}'")

if not repo_server:
    fail("DRIFT_REPO_SERVER is empty. Edit settings.env and set it to your SQL Server hostname or IP.")
    sys.exit(1)

# ── 2. Check driver installed ──────────────────────────────────
sep()
print("  Step 2: Checking ODBC driver")
sep()
try:
    import pyodbc
    available = [d for d in pyodbc.drivers() if "SQL Server" in d]
    if available:
        for d_name in available:
            ok(f"Found driver: {d_name}")
        if driver not in available:
            warn(f"settings.env specifies '{driver}' but it is not in the list above.")
            warn(f"Update DRIFT_ODBC_DRIVER in settings.env to one of the drivers shown.")
    else:
        fail("No SQL Server ODBC driver found. Install msodbcsql.msi first.")
        sys.exit(1)
except ImportError:
    fail("pyodbc not importable. Re-run bootstrap.bat.")
    sys.exit(1)

# ── 3. Parse host / port from server string ────────────────────
sep()
print("  Step 3: Parsing server string")
sep()
raw = repo_server.strip()

# Strip tcp: prefix for DNS test
host_for_dns = raw.replace("tcp:", "").split("\\")[0].split(",")[0].strip()
port_for_test = 1433

# Check for explicit port  e.g.  SERVER,1455  or  tcp:SERVER,1455
if "," in raw.split("\\")[0]:
    try:
        port_for_test = int(raw.split("\\")[0].split(",")[1].strip())
    except Exception:
        pass

info(f"Raw server string : '{raw}'")
info(f"Host for DNS/ping : '{host_for_dns}'")
info(f"Port to test      : {port_for_test}")

# ── 4. DNS resolution ──────────────────────────────────────────
sep()
print("  Step 4: DNS resolution")
sep()
try:
    ip = socket.gethostbyname(host_for_dns)
    ok(f"Resolved '{host_for_dns}' -> {ip}")
except socket.gaierror as e:
    fail(f"Cannot resolve '{host_for_dns}': {e}")
    print()
    print("  Fix options:")
    print(f"    A) Use the IP address directly in settings.env:")
    print(f"       DRIFT_REPO_SERVER=10.x.x.x")
    print(f"    B) Check that DNS is configured on this server")
    print(f"    C) Add an entry to C:\\Windows\\System32\\drivers\\etc\\hosts")
    sys.exit(1)

# ── 5. TCP port check ──────────────────────────────────────────
sep()
print(f"  Step 5: TCP port {port_for_test} reachability")
sep()
try:
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(5)
    result = sock.connect_ex((host_for_dns, port_for_test))
    sock.close()
    if result == 0:
        ok(f"Port {port_for_test} on {host_for_dns} is open")
    else:
        fail(f"Port {port_for_test} on {host_for_dns} is CLOSED or blocked (error {result})")
        print()
        print("  Fix options:")
        print("    A) Check firewall rules allow TCP 1433 from this machine")
        print("    B) If SQL Server uses a non-standard port, add it to settings.env:")
        print("       DRIFT_REPO_SERVER=SQLPROD01,1455")
        print("    C) Ensure SQL Server Browser service is running (for named instances)")
        sys.exit(1)
except Exception as e:
    fail(f"Socket error: {e}")
    sys.exit(1)

# ── 6. Build and show connection string ────────────────────────
sep()
print("  Step 6: Building connection string")
sep()
if trusted:
    conn_str = (
        f"DRIVER={{{driver}}};"
        f"SERVER={raw};"
        f"DATABASE={repo_db};"
        f"Connect Timeout={timeout};"
        f"Trusted_Connection=yes;"
    )
    info("Auth: Windows Integrated (Trusted_Connection=yes)")
else:
    conn_str = (
        f"DRIVER={{{driver}}};"
        f"SERVER={raw};"
        f"DATABASE={repo_db};"
        f"Connect Timeout={timeout};"
        f"UID={sql_user};PWD=***;"
    )
    info(f"Auth: SQL Server (UID={sql_user})")

# Print without password
info(f"Connection string: {conn_str}")

# ── 7. Actual connection test ──────────────────────────────────
sep()
print("  Step 7: Connecting to SQL Server")
sep()
real_conn_str = conn_str.replace("PWD=***", f"PWD={sql_pwd}")
try:
    import pyodbc
    conn = pyodbc.connect(real_conn_str, autocommit=True, timeout=15)
    row = conn.cursor().execute("SELECT @@SERVERNAME, @@VERSION").fetchone()
    conn.close()
    ok(f"Connected! Server name: {row[0]}")
    ok(f"Version: {row[1].splitlines()[0]}")
except pyodbc.Error as e:
    fail(f"Connection failed: {e}")
    print()
    print("  Diagnostic: the TCP port was open but the SQL connection was refused.")
    print("  Common causes:")
    print("    A) Windows auth (Trusted_Connection) failing:")
    print("       - This machine's computer account may not have SQL access")
    print("       - Try SQL auth: set DRIFT_TRUSTED=0 and set DRIFT_SQL_USER/PWD")
    print("    B) The database does not exist yet:")
    print("       - Run .internal\\schema.sql in SSMS first")
    print("    C) Named instance not specified:")
    print("       - Use DRIFT_REPO_SERVER=HOSTNAME\\INSTANCENAME")
    sys.exit(1)

# ── 8. Check DriftDetector database ───────────────────────────
sep()
print("  Step 8: Checking DriftDetector database and tables")
sep()
try:
    conn = pyodbc.connect(real_conn_str, autocommit=True, timeout=15)
    cur = conn.cursor()

    cur.execute("SELECT name FROM sys.databases WHERE name = ?", repo_db)
    if cur.fetchone():
        ok(f"Database '{repo_db}' exists")
    else:
        fail(f"Database '{repo_db}' does not exist")
        print("  Run .internal\\schema.sql in SSMS to create it.")
        conn.close()
        sys.exit(1)

    for table in ("CIS_Baselines", "Fact_Audit_History"):
        cur.execute(
            "SELECT 1 FROM INFORMATION_SCHEMA.TABLES "
            "WHERE TABLE_NAME=? AND TABLE_SCHEMA='dbo'", table
        )
        if cur.fetchone():
            ok(f"Table dbo.{table} exists")
        else:
            fail(f"Table dbo.{table} NOT FOUND")
            print("  Run .internal\\schema.sql in SSMS to create it.")

    cur.execute("SELECT COUNT(*) FROM dbo.CIS_Baselines WHERE IsActive=1")
    count = cur.fetchone()[0]
    if count > 0:
        ok(f"CIS_Baselines has {count} active rules")
    else:
        fail("CIS_Baselines is empty -- run .internal\\schema.sql to seed baseline data")

    conn.close()
except pyodbc.Error as e:
    fail(f"Database check failed: {e}")
    sys.exit(1)

# ── Done ───────────────────────────────────────────────────────
sep()
print()
print("  All checks passed. The repository connection is working correctly.")
print("  You can now run the audit.")
print()

input("  Press Enter to close.")
