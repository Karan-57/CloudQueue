#!/usr/bin/env bash
# ==============================================================================
# CloudQueue - Phase 7: Real-GCP End-to-End Validation & Benchmark Script
# Usage: ./gcp/run_phase7.sh [OPTIONS]
# ==============================================================================

set -eo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$PROJECT_ROOT"

# Detect Python executable
if command -v python3 &>/dev/null; then
    PYTHON_CMD="python3"
elif command -v python &>/dev/null; then
    PYTHON_CMD="python"
else
    echo "Error: Python 3 executable not found in PATH." >&2
    exit 1
fi

# Forward all arguments directly to Python runner
exec "$PYTHON_CMD" "$PROJECT_ROOT/gcp/run_phase7.py" "$@"
