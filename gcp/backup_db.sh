#!/usr/bin/env bash
# ==============================================================================
# CloudQueue - Phase 5: Persistent SQLite Database Backup Script
# Periodically backs up the authoritative SQLite database to GitHub.
# Safe with active SQLite WAL mode (uses sqlite3.Connection.backup()).
# Only commits and pushes when database changes are detected.
# ==============================================================================

set -eo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$PROJECT_ROOT"

LOG_DIR="$PROJECT_ROOT/.gcp"
LOG_FILE="$LOG_DIR/backup.log"
mkdir -p "$LOG_DIR"

log() {
    local msg="$1"
    local timestamp
    timestamp="$(date '+%Y-%m-%d %H:%M:%S')"
    echo "[$timestamp] $msg"
}

# ------------------------------------------------------------------------------
# 1. Cron Helper Options (--install-cron / --remove-cron)
# ------------------------------------------------------------------------------
if [ "$1" = "--install-cron" ]; then
    echo "Installing CloudQueue database backup cron job (every 5 minutes)..."
    CRON_ENTRY="*/5 * * * * $SCRIPT_DIR/backup_db.sh >> $LOG_FILE 2>&1 # CLOUDQUEUE_DB_BACKUP"

    # Read existing crontab
    EXISTING_CRON="$(crontab -l 2>/dev/null || true)"

    # Remove any pre-existing entry with marker # CLOUDQUEUE_DB_BACKUP
    FILTERED_CRON="$(echo "$EXISTING_CRON" | grep -v "# CLOUDQUEUE_DB_BACKUP" || true)"

    # Append the new single entry
    if [ -n "$FILTERED_CRON" ]; then
        NEW_CRONTAB="${FILTERED_CRON}
${CRON_ENTRY}"
    else
        NEW_CRONTAB="${CRON_ENTRY}"
    fi

    echo "$NEW_CRONTAB" | crontab -
    echo "Crontab installed successfully:"
    echo "  $CRON_ENTRY"
    exit 0
fi

if [ "$1" = "--remove-cron" ]; then
    echo "Removing CloudQueue database backup cron job..."
    EXISTING_CRON="$(crontab -l 2>/dev/null || true)"
    FILTERED_CRON="$(echo "$EXISTING_CRON" | grep -v "# CLOUDQUEUE_DB_BACKUP" || true)"
    if [ -n "$FILTERED_CRON" ]; then
        echo "$FILTERED_CRON" | crontab -
    else
        crontab -r 2>/dev/null || true
    fi
    echo "CloudQueue cron job removed."
    exit 0
fi

if [ "$1" = "--help" ] || [ "$1" = "-h" ]; then
    echo "Usage: ./gcp/backup_db.sh [OPTIONS]"
    echo ""
    echo "Options:"
    echo "  (no arguments)   Execute database backup, commit if changed, and push to GitHub."
    echo "  --install-cron   Install/update user crontab job to run every 5 minutes."
    echo "  --remove-cron    Remove the CloudQueue backup job from user crontab."
    echo "  --help, -h       Display this help message."
    exit 0
fi

# ------------------------------------------------------------------------------
# 2. Detect Python & Database Path
# ------------------------------------------------------------------------------
log "Starting database backup"

if command -v python3 &>/dev/null; then
    PYTHON_CMD="python3"
elif command -v python &>/dev/null; then
    PYTHON_CMD="python"
else
    log "Error: Python 3 executable not found."
    exit 1
fi

# Determine configured database path via config.py (respecting .env)
DB_REL_PATH="$("$PYTHON_CMD" -c "import config; print(config.DATABASE_PATH)" 2>/dev/null || echo "database/cloudqueue.db")"
DB_SRC_PATH="$PROJECT_ROOT/$DB_REL_PATH"

if [ ! -f "$DB_SRC_PATH" ]; then
    log "Error: Source database '$DB_SRC_PATH' does not exist."
    echo "Error: Configured database file '$DB_SRC_PATH' does not exist. Nothing to backup." >&2
    exit 1
fi

# ------------------------------------------------------------------------------
# 3. Create Consistent SQLite Backup (WAL-Safe)
# ------------------------------------------------------------------------------
BACKUP_DIR="$PROJECT_ROOT/backups"
BACKUP_FILE="$BACKUP_DIR/cloudqueue.db"
TMP_BACKUP_FILE="$BACKUP_DIR/cloudqueue.db.tmp"

mkdir -p "$BACKUP_DIR"

# Perform atomic backup using Python sqlite3.Connection.backup()
if ! "$PYTHON_CMD" -c "
import sqlite3, sys, os

src_path = sys.argv[1]
dst_path = sys.argv[2]

if not os.path.exists(src_path):
    sys.exit(1)

# Open source read-only to ensure active worker transactions are never locked
src_conn = sqlite3.connect(f'file:{os.path.abspath(src_path)}?mode=ro', uri=True, timeout=30.0)
dst_conn = sqlite3.connect(dst_path, timeout=30.0)

try:
    src_conn.backup(dst_conn)
finally:
    dst_conn.close()
    src_conn.close()
" "$DB_SRC_PATH" "$TMP_BACKUP_FILE"; then
    log "Error: SQLite native backup failed."
    rm -f "$TMP_BACKUP_FILE"
    echo "Error: SQLite backup execution failed." >&2
    exit 1
fi

# Verify the temporary backup is a valid SQLite database with expected schema
if ! "$PYTHON_CMD" -c "
import sqlite3, sys
conn = sqlite3.connect(sys.argv[1])
cursor = conn.cursor()
cursor.execute(\"SELECT name FROM sqlite_master WHERE type='table'\")
tables = {r[0] for r in cursor.fetchall()}
conn.close()
required = {'jobs', 'experiments', 'experiment_jobs'}
if not required.issubset(tables):
    sys.exit(2)
" "$TMP_BACKUP_FILE" 2>/dev/null; then
    log "Error: Backup database integrity verification failed."
    rm -f "$TMP_BACKUP_FILE"
    echo "Error: Backup database verification failed." >&2
    exit 1
fi

# Atomically move the verified backup into place
mv "$TMP_BACKUP_FILE" "$BACKUP_FILE"

# ------------------------------------------------------------------------------
# 4. Check for Database Changes
# ------------------------------------------------------------------------------
# Check if the backup file differs from git index
if [ -z "$(git status --short -- "backups/cloudqueue.db" 2>/dev/null || true)" ]; then
    log "No database changes detected"
    echo "No database changes detected. Nothing to commit."
    exit 0
fi

log "Database changed"

# ------------------------------------------------------------------------------
# 5. Check Git Remote Configuration
# ------------------------------------------------------------------------------
if ! git remote -v 2>/dev/null | grep -q .; then
    log "Error: No Git remote configured."
    echo ""
    echo "No Git remote is configured." >&2
    echo "Configure the GitHub remote before enabling automated backups." >&2
    exit 1
fi

# ------------------------------------------------------------------------------
# 6. Stage ONLY backups/cloudqueue.db & Commit
# ------------------------------------------------------------------------------
# Ensure git committer identity is present on fresh VMs
if [ -z "$(git config user.name 2>/dev/null || true)" ]; then
    git config user.name "CloudQueue Backup Bot"
fi
if [ -z "$(git config user.email 2>/dev/null || true)" ]; then
    git config user.email "backup@cloudqueue.local"
fi

# Strict safety: ONLY stage the backup file (never git add .)
git add "backups/cloudqueue.db"

COMMIT_MSG="Update CloudQueue database backup - $(date '+%Y-%m-%d %H:%M:%S')"
git commit -m "$COMMIT_MSG"
log "Commit created"

# ------------------------------------------------------------------------------
# 7. Push to Configured Remote
# ------------------------------------------------------------------------------
# Detect remote and branch
UPSTREAM_REMOTE="$(git rev-parse --abbrev-ref --symbolic-full-name @{u} 2>/dev/null | cut -d/ -f1 || true)"
if [ -z "$UPSTREAM_REMOTE" ]; then
    UPSTREAM_REMOTE="$(git remote 2>/dev/null | head -n 1 || echo "origin")"
fi

CURRENT_BRANCH="$(git rev-parse --abbrev-ref HEAD 2>/dev/null || echo "main")"

log "Pushing backup to $UPSTREAM_REMOTE/$CURRENT_BRANCH"

if ! git push "$UPSTREAM_REMOTE" "$CURRENT_BRANCH" 2>/dev/null; then
    log "Error: Git push failed."
    echo ""
    echo "Git push failed." >&2
    echo "" >&2
    echo "Configure GitHub authentication for this VM, then run:" >&2
    echo "  ./gcp/backup_db.sh" >&2
    exit 1
fi

log "Push successful"
echo "Backup successfully pushed."
