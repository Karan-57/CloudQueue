#!/usr/bin/env bash

set -eo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$PROJECT_ROOT"

BACKUP_SCRIPT="$PROJECT_ROOT/gcp/backup_db.sh"
LOG_DIR="$PROJECT_ROOT/.gcp/logs"
LOG_FILE="$LOG_DIR/backup_cron.log"
mkdir -p "$LOG_DIR"

if [ ! -f "$BACKUP_SCRIPT" ]; then
    echo "Error: Backup script not found at $BACKUP_SCRIPT" >&2
    exit 1
fi

chmod +x "$BACKUP_SCRIPT" 2>/dev/null || true

if ! command -v crontab &>/dev/null; then
    echo "Error: 'crontab' command is not available in PATH." >&2
    exit 1
fi

CRON_TAG="# CloudQueue-Backup-Cron"
CRON_SCHEDULE="*/5 * * * *"
CRON_JOB="$CRON_SCHEDULE /bin/bash \"$BACKUP_SCRIPT\" >> \"$LOG_FILE\" 2>&1 $CRON_TAG"

CURRENT_USER="$(whoami 2>/dev/null || id -un 2>/dev/null || echo "user")"
echo "Configuring backup cron job for user: $CURRENT_USER..."

EXISTING_CRON="$(crontab -l 2>/dev/null || true)"

FILTERED_CRON="$(printf "%s\n" "$EXISTING_CRON" | grep -v "backup_db.sh" | grep -v "CloudQueue-Backup-Cron" || true)"

if [ -n "$FILTERED_CRON" ]; then
    NEW_CRON="$(printf "%s\n%s\n" "$FILTERED_CRON" "$CRON_JOB")"
else
    NEW_CRON="$(printf "%s\n" "$CRON_JOB")"
fi

printf "%s\n" "$NEW_CRON" | crontab -

echo "Cron job installed successfully."
echo "Installed Schedule: $CRON_SCHEDULE"
echo "Backup Command: /bin/bash \"$BACKUP_SCRIPT\" >> \"$LOG_FILE\" 2>&1"
