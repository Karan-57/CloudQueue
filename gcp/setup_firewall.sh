#!/usr/bin/env bash
# ==============================================================================
# CloudQueue: GCP Firewall Rule Setup
# Creates/configures a GCP firewall rule for port 5000:
# - Direction: INGRESS
# - Target: All instances in network
# - Source ranges: 0.0.0.0/0
# - Protocol/port: TCP 5000
# - Rule name: cloudqueue-allow-5000
# Idempotent: checks for existence before attempting creation.
# ==============================================================================

set -eo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$PROJECT_ROOT"

RULE_NAME="cloudqueue-allow-5000"

echo "======================================================"
echo " CloudQueue: GCP Firewall Rule Setup"
echo " Rule Name: $RULE_NAME"
echo "======================================================"

# 1. Detect GCP Project
if ! command -v gcloud &>/dev/null; then
    echo "Error: 'gcloud' CLI is not found in PATH." >&2
    exit 1
fi

PROJECT_ID="${GOOGLE_CLOUD_PROJECT:-$(gcloud config get-value project 2>/dev/null | tr -d '[:space:]')}"
if [ -z "$PROJECT_ID" ] || [ "$PROJECT_ID" = "(unset)" ]; then
    echo "Error: No active Google Cloud project configured." >&2
    echo "Please configure your GCP project using: gcloud config set project <PROJECT_ID>" >&2
    exit 1
fi

echo "Active GCP Project: $PROJECT_ID"

# 2. Check if firewall rule already exists
echo "Checking if firewall rule '$RULE_NAME' already exists..."
if gcloud compute firewall-rules describe "$RULE_NAME" --project="$PROJECT_ID" &>/dev/null; then
    echo "Firewall rule '$RULE_NAME' already exists and is configured."
    exit 0
fi

# 3. Create firewall rule
echo "Firewall rule '$RULE_NAME' not found. Creating rule..."
if ! gcloud compute firewall-rules create "$RULE_NAME" \
    --direction=INGRESS \
    --priority=1000 \
    --network=default \
    --action=ALLOW \
    --rules=tcp:5000 \
    --source-ranges=0.0.0.0/0 \
    --project="$PROJECT_ID" \
    --description="Allow incoming TCP port 5000 for CloudQueue dashboard"; then

    echo "" >&2
    echo "Error: Failed to create GCP firewall rule '$RULE_NAME'." >&2
    echo "If your Google Cloud Skills Boost environment does not permit firewall creation," >&2
    echo "please create or request the firewall rule manually via the GCP Console with:" >&2
    echo "  - Name: $RULE_NAME" >&2
    echo "  - Direction: INGRESS" >&2
    echo "  - Action on match: Allow" >&2
    echo "  - Targets: All instances in the network" >&2
    echo "  - Source filter: IPv4 ranges" >&2
    echo "  - Source IPv4 ranges: 0.0.0.0/0" >&2
    echo "  - Protocols and ports: Specified protocols and ports -> TCP: 5000" >&2
    exit 1
fi

echo "Firewall rule '$RULE_NAME' created successfully."
