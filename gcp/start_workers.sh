#!/usr/bin/env bash
# ==============================================================================
# CloudQueue - Phase 4: Automated Worker Start Script
# Starts 1, 2, 4, or 8 background worker processes on a single GCP VM.
# All workers consume from the shared Pub/Sub subscription (cloudqueue-worker-sub).
# ==============================================================================

set -eo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$PROJECT_ROOT"

PID_DIR="$PROJECT_ROOT/.gcp"
LOG_DIR="$PROJECT_ROOT/.gcp/logs"
mkdir -p "$PID_DIR" "$LOG_DIR"

# ------------------------------------------------------------------------------
# 1. Validate Worker Count
# ------------------------------------------------------------------------------
WORKER_COUNT="$1"

case "$WORKER_COUNT" in
    1|2|4|8)
        ;;
    *)
        echo "Invalid worker count." >&2
        echo "Supported worker counts: 1, 2, 4, 8" >&2
        exit 1
        ;;
esac

# ------------------------------------------------------------------------------
# Helper: Check if Process is Alive
# ------------------------------------------------------------------------------
is_pid_running() {
    local pid="$1"
    [ -z "$pid" ] && return 1
    # Standard Linux / POSIX check
    if kill -0 "$pid" 2>/dev/null; then
        return 0
    fi
    # Windows fallback for testing in Git Bash
    if command -v tasklist.exe &>/dev/null; then
        if tasklist.exe 2>/dev/null | grep -w "$pid" >/dev/null; then
            return 0
        fi
    fi
    return 1
}

# ------------------------------------------------------------------------------
# 2. Prevent Accidental Duplicate Launches
# ------------------------------------------------------------------------------
shopt -s nullglob
EXISTING_PIDS=("$PID_DIR"/worker-*.pid)

ACTIVE_WORKERS=0
for pid_file in "${EXISTING_PIDS[@]}"; do
    if [ -f "$pid_file" ]; then
        PID="$(head -n 1 "$pid_file" 2>/dev/null | tr -d '[:space:]')"
        if [ -n "$PID" ] && is_pid_running "$PID"; then
            ACTIVE_WORKERS=$((ACTIVE_WORKERS + 1))
        else
            # Stale PID file; clean it up
            rm -f "$pid_file"
        fi
    fi
done

if [ "$ACTIVE_WORKERS" -gt 0 ]; then
    echo "CloudQueue workers are already running." >&2
    echo "Use ./gcp/stop_workers.sh before starting a new worker count." >&2
    exit 1
fi

# ------------------------------------------------------------------------------
# 3. Detect Python Executable
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
# 4. Launch Workers
# ------------------------------------------------------------------------------
echo "Starting $WORKER_COUNT CloudQueue worker(s) in GCP mode..."

for ((i = 1; i <= WORKER_COUNT; i++)); do
    WORKER_ID="Worker-$i"
    PID_FILE="$PID_DIR/worker-$i.pid"
    LOG_FILE="$LOG_DIR/worker-$i.log"

    # Start worker process in background
    nohup "$PYTHON_CMD" worker.py --worker-id "$WORKER_ID" --worker-mode normal --mode gcp >> "$LOG_FILE" 2>&1 &
    WORKER_PID=$!
    echo "$WORKER_PID" > "$PID_FILE"

    # Brief check that the process didn't immediately crash on startup
    sleep 0.15
    if is_pid_running "$WORKER_PID"; then
        echo "  $WORKER_ID started (PID: $WORKER_PID, log: .gcp/logs/worker-$i.log)"
    else
        echo "  Warning: $WORKER_ID (PID: $WORKER_PID) exited immediately. Check $LOG_FILE" >&2
    fi
done

echo ""
echo "Successfully started $WORKER_COUNT CloudQueue worker(s)."
echo "Shared Subscription: cloudqueue-worker-sub"
echo "Inspect logs via: tail -f .gcp/logs/worker-1.log"
echo "Stop workers via: ./gcp/stop_workers.sh"
