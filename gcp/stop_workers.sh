#!/usr/bin/env bash

set -eo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$PROJECT_ROOT"

PID_DIR="$PROJECT_ROOT/.gcp"

echo "Stopping CloudQueue workers..."

is_pid_running() {
    local pid="$1"
    [ -z "$pid" ] && return 1
    if kill -0 "$pid" 2>/dev/null; then
        return 0
    fi
    if command -v tasklist.exe &>/dev/null; then
        if tasklist.exe 2>/dev/null | grep -w "$pid" >/dev/null; then
            return 0
        fi
    fi
    return 1
}

shopt -s nullglob
PID_FILES=("$PID_DIR"/worker-*.pid)

if [ "${#PID_FILES[@]}" -eq 0 ]; then
    echo "No running CloudQueue workers found."
    exit 0
fi

STOPPED_COUNT=0

for pid_file in "${PID_FILES[@]}"; do
    if [ -f "$pid_file" ]; then
        PID="$(head -n 1 "$pid_file" 2>/dev/null | tr -d '[:space:]')"
        WORKER_NUM="$(basename "$pid_file" .pid | sed 's/worker-//')"
        WORKER_ID="Worker-$WORKER_NUM"

        if [ -n "$PID" ] && is_pid_running "$PID"; then
            kill -15 "$PID" 2>/dev/null || kill "$PID" 2>/dev/null || true

            EXITED=0
            for attempt in {1..25}; do
                if ! is_pid_running "$PID"; then
                    EXITED=1
                    break
                fi
                sleep 0.2
            done

            if [ "$EXITED" -eq 0 ]; then
                echo "  $WORKER_ID did not exit gracefully, sending SIGKILL..."
                kill -9 "$PID" 2>/dev/null || true
                if command -v taskkill.exe &>/dev/null; then
                    taskkill.exe //PID "$PID" //F &>/dev/null || true
                fi
            fi

            echo "$WORKER_ID stopped."
            STOPPED_COUNT=$((STOPPED_COUNT + 1))
        else
            echo "$WORKER_ID was not running (stale PID $PID)."
        fi

        rm -f "$pid_file"
    fi
done

echo ""
if [ "$STOPPED_COUNT" -gt 0 ]; then
    echo "All CloudQueue workers stopped."
else
    echo "No active CloudQueue workers were running. PID files cleaned up."
fi
