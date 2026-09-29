#!/usr/bin/env bash
# ==============================================================================
# CloudQueue - Phase 6: Restore GCP Results Database
# Restores the SQLite database persisted by the GCP backup system (backups/cloudqueue.db)
# into the local working database (database/cloudqueue.db).
#
# Safety features:
# 1. Detects running Flask or worker processes to prevent database corruption.
# 2. Validates backup database integrity and expected schema before touching target.
# 3. Creates a timestamped pre-restore safety copy of the current database.
# 4. Uses SQLite native backup API to ensure clean WAL-free standalone target.
# 5. Never modifies backups/cloudqueue.db.
# ==============================================================================

set -eo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$PROJECT_ROOT"

# ------------------------------------------------------------------------------
# 1. Detect Python Executable
# ------------------------------------------------------------------------------
if command -v python3 &>/dev/null; then
    PYTHON_CMD="python3"
elif command -v python &>/dev/null; then
    PYTHON_CMD="python"
else
    echo "Error: Python 3 executable not found in PATH." >&2
    exit 1
fi

# ------------------------------------------------------------------------------
# 2. Check for Active CloudQueue Processes
# ------------------------------------------------------------------------------
# A. Check for running worker PID files in .gcp/
shopt -s nullglob
ACTIVE_WORKER_FOUND=0
for pid_file in .gcp/worker-*.pid; do
    if [ -f "$pid_file" ]; then
        PID="$(cat "$pid_file" 2>/dev/null | tr -d '[:space:]')"
        if [ -n "$PID" ]; then
            if kill -0 "$PID" 2>/dev/null; then
                ACTIVE_WORKER_FOUND=1
                break
            fi
            if command -v tasklist.exe &>/dev/null; then
                if tasklist.exe 2>/dev/null | grep -w "$PID" >/dev/null; then
                    ACTIVE_WORKER_FOUND=1
                    break
                fi
            fi
        fi
    fi
done

if [ "$ACTIVE_WORKER_FOUND" -eq 1 ]; then
    echo "Error: CloudQueue worker processes are currently running." >&2
    echo "Please stop all workers (e.g. via ./gcp/stop_workers.sh) before restoring the database." >&2
    exit 1
fi

# B. Check if Flask web application is actively listening on configured port
PORT_IN_USE="$("$PYTHON_CMD" -c "
import socket, sys
try:
    import config
    port = getattr(config, 'PORT', 5000)
except Exception:
    port = 5000

s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.settimeout(0.3)
try:
    result = s.connect_ex(('127.0.0.1', port))
    if result == 0:
        print(f'IN_USE:{port}')
except Exception:
    pass
finally:
    s.close()
" 2>/dev/null || true)"

if [[ "$PORT_IN_USE" == IN_USE:* ]]; then
    PORT_NUM="${PORT_IN_USE#IN_USE:}"
    echo "Error: CloudQueue application appears to be running on port $PORT_NUM." >&2
    echo "Please stop the local CloudQueue application before restoring the database." >&2
    exit 1
fi

# ------------------------------------------------------------------------------
# 3. Verify Backup Database Existence & Integrity
# ------------------------------------------------------------------------------
BACKUP_SRC="backups/cloudqueue.db"

if [ ! -f "$BACKUP_SRC" ]; then
    echo "Error: Backup database '$BACKUP_SRC' does not exist." >&2
    echo "Ensure you have pulled the latest changes from GitHub (e.g. git pull)." >&2
    exit 1
fi

# Validate that the backup is a valid SQLite DB and contains required CloudQueue tables
VALIDATION_OUT="$("$PYTHON_CMD" -c "
import sqlite3, sys, os

src_path = sys.argv[1]
try:
    conn = sqlite3.connect(f'file:{os.path.abspath(src_path)}?mode=ro', uri=True, timeout=10.0)
    cursor = conn.cursor()

    # Integrity check
    cursor.execute('PRAGMA integrity_check;')
    res = cursor.fetchone()
    if not res or res[0] != 'ok':
        print('CORRUPT_DB')
        sys.exit(0)

    # Required tables check
    cursor.execute(\"SELECT name FROM sqlite_master WHERE type='table';\")
    tables = {r[0] for r in cursor.fetchall()}
    required = {'jobs', 'experiments', 'experiment_jobs'}
    if not required.issubset(tables):
        print('MISSING_TABLES:' + ','.join(required - tables))
        sys.exit(0)

    # Count records for reporting
    cursor.execute('SELECT COUNT(*) FROM jobs;')
    j_count = cursor.fetchone()[0]
    cursor.execute('SELECT COUNT(*) FROM experiments;')
    e_count = cursor.fetchone()[0]
    cursor.execute('SELECT COUNT(*) FROM experiment_jobs;')
    ej_count = cursor.fetchone()[0]
    conn.close()

    print(f'VALID:{j_count}:{e_count}:{ej_count}')
except Exception as e:
    print(f'ERROR:{e}')
" "$BACKUP_SRC" 2>/dev/null || echo "ERROR:Python validation script failed")"

case "$VALIDATION_OUT" in
    VALID:*)
        IFS=':' read -r _ J_COUNT E_COUNT EJ_COUNT <<< "$VALIDATION_OUT"
        ;;
    CORRUPT_DB)
        echo "Error: '$BACKUP_SRC' failed SQLite integrity check (corrupted database file)." >&2
        exit 1
        ;;
    MISSING_TABLES:*)
        echo "Error: '$BACKUP_SRC' is missing required tables: ${VALIDATION_OUT#MISSING_TABLES:}" >&2
        exit 1
        ;;
    *)
        echo "Error validating backup database '$BACKUP_SRC': $VALIDATION_OUT" >&2
        exit 1
        ;;
esac

# ------------------------------------------------------------------------------
# 4. Determine Target Database Path & Create Pre-Restore Safety Copy
# ------------------------------------------------------------------------------
TARGET_DB="$("$PYTHON_CMD" -c "import os, config; print(os.path.normpath(config.DATABASE_PATH).replace('\\\\', '/'))" 2>/dev/null || echo "database/cloudqueue.db")"
TARGET_DIR="$(dirname "$TARGET_DB")"
mkdir -p "$TARGET_DIR"

LOCAL_BACKUP_DIR=".gcp/local_db_backups"
mkdir -p "$LOCAL_BACKUP_DIR"

if [ -f "$TARGET_DB" ]; then
    TIMESTAMP="$(date '+%Y%m%d_%H%M%S')"
    PRE_RESTORE_BACKUP="$LOCAL_BACKUP_DIR/cloudqueue_pre_restore_${TIMESTAMP}.db"

    # Safely back up the current active database using SQLite native backup
    "$PYTHON_CMD" -c "
import sqlite3, sys, os
src_path = sys.argv[1]
dst_path = sys.argv[2]
src = sqlite3.connect(f'file:{os.path.abspath(src_path)}?mode=ro', uri=True, timeout=30.0)
dst = sqlite3.connect(dst_path, timeout=30.0)
try:
    src.backup(dst)
finally:
    dst.close()
    src.close()
" "$TARGET_DB" "$PRE_RESTORE_BACKUP"

    echo "Created pre-restore safety copy of current local database:"
    echo "  $PRE_RESTORE_BACKUP"
fi

# ------------------------------------------------------------------------------
# 5. Safely Restore Database to Target
# ------------------------------------------------------------------------------
TMP_TARGET="$TARGET_DB.tmp.$$"

# Use SQLite native backup from backups/cloudqueue.db into temporary destination
if ! "$PYTHON_CMD" -c "
import sqlite3, sys, os
src_path = sys.argv[1]
dst_path = sys.argv[2]

src = sqlite3.connect(f'file:{os.path.abspath(src_path)}?mode=ro', uri=True, timeout=30.0)
dst = sqlite3.connect(dst_path, timeout=30.0)
try:
    src.backup(dst)
finally:
    dst.close()
    src.close()
" "$BACKUP_SRC" "$TMP_TARGET"; then
    echo "Error: Failed to create restored database from '$BACKUP_SRC'." >&2
    rm -f "$TMP_TARGET"
    exit 1
fi

# Verify restored temporary database integrity before replacing active file
if ! "$PYTHON_CMD" -c "
import sqlite3, sys
conn = sqlite3.connect(sys.argv[1])
cursor = conn.cursor()
cursor.execute('PRAGMA integrity_check;')
res = cursor.fetchone()
conn.close()
if not res or res[0] != 'ok':
    sys.exit(1)
" "$TMP_TARGET" 2>/dev/null; then
    echo "Error: Restored database failed integrity verification. Aborting replacement." >&2
    rm -f "$TMP_TARGET"
    exit 1
fi

# Clean up any lingering active WAL/SHM files for the target database
rm -f "${TARGET_DB}-wal" "${TARGET_DB}-shm"

# Atomically replace target database
mv "$TMP_TARGET" "$TARGET_DB"

# ------------------------------------------------------------------------------
# 6. Success Report
# ------------------------------------------------------------------------------
echo ""
echo "======================================================"
echo " CloudQueue: Database Restore Successful"
echo "======================================================"
echo "  Source : $BACKUP_SRC"
echo "  Target : $TARGET_DB"
echo ""
echo "Restored Contents:"
echo "  - Standard Jobs   : $J_COUNT"
echo "  - Experiments     : $E_COUNT"
echo "  - Experiment Jobs : $EJ_COUNT"
echo ""
echo "You can now launch the local CloudQueue application to view"
echo "restored experiments, charts, and metrics in your browser."
echo "======================================================"
