#!/usr/bin/env bash
# ==============================================================================
# CloudQueue - Automated GCP Setup Script
# Configures a fresh Google Cloud Skills Boost VM / Debian / Ubuntu environment:
# 1. Verifies/installs Git (apt install if missing with sudo/root)
# 2. Detects active GCP Project via gcloud
# 3. Verifies Python 3 availability
# 4. Bootstraps pip locally without requiring sudo or root access
# 5. Installs requirements.txt (handles PEP 668 user environments)
# 6. Creates/verifies Pub/Sub topic (cloudqueue-jobs)
# 7. Creates/verifies Pub/Sub subscription (cloudqueue-worker-sub)
# 8. Configures CloudQueue GCP mode (.env)
# 9. Initializes SQLite database and verifies required tables
# 10. Configures GCP firewall rule (cloudqueue-allow-5000)
# 11. Installs database backup cron job (every 5 minutes)
# 12. Performs lightweight verification and displays safe summary
# ==============================================================================

set -eo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$PROJECT_ROOT"

echo "======================================================"
echo " CloudQueue: Automated GCP Setup"
echo " Project root: $PROJECT_ROOT"
echo "======================================================"

# ------------------------------------------------------------------------------
# 1. Check / Install Git
# ------------------------------------------------------------------------------
echo ""
echo "==> Step 1: Checking Git installation..."
if ! command -v git &>/dev/null; then
    echo "git is not installed. Attempting installation via apt..."
    if command -v sudo &>/dev/null && command -v apt &>/dev/null; then
        sudo apt update
        sudo apt install -y git
    elif command -v apt &>/dev/null && [ "$(id -u 2>/dev/null || echo 1)" -eq 0 ]; then
        apt update
        apt install -y git
    else
        echo "Error: 'git' is not installed and sudo/apt is unavailable." >&2
        echo "Please install Git manually on this machine." >&2
        exit 1
    fi

    if ! command -v git &>/dev/null; then
        echo "Error: Failed to install git. Please install Git manually." >&2
        exit 1
    fi
    echo "Git installed successfully: $(git --version)"
else
    echo "Git is already installed: $(git --version)"
fi

# ------------------------------------------------------------------------------
# 2. Detect GCP Project via gcloud
# ------------------------------------------------------------------------------
echo ""
echo "==> Step 2: Detecting GCP Project..."
if ! command -v gcloud &>/dev/null; then
    echo "Error: 'gcloud' CLI is not found in PATH." >&2
    echo "Please ensure the Google Cloud SDK is installed and available." >&2
    exit 1
fi

PROJECT_ID="$(gcloud config get-value project 2>/dev/null | tr -d '[:space:]')"
if [ -z "$PROJECT_ID" ] || [ "$PROJECT_ID" = "(unset)" ]; then
    echo "Error: No Google Cloud project is configured in gcloud." >&2
    echo "Please select or configure your active project using:" >&2
    echo "  gcloud config set project <PROJECT_ID>" >&2
    exit 1
fi
echo "Active GCP Project: $PROJECT_ID"

# ------------------------------------------------------------------------------
# 3. Detect Python 3 (no sudo assumed)
# ------------------------------------------------------------------------------
echo ""
echo "==> Step 3: Checking Python 3..."
if ! command -v python3 &>/dev/null; then
    echo "Error: 'python3' executable not found in PATH." >&2
    echo "Please ensure Python 3 is installed." >&2
    exit 1
fi
PYTHON_VERSION="$(python3 --version 2>&1)"
echo "Detected: $PYTHON_VERSION"

# ------------------------------------------------------------------------------
# 4. Handle pip safely without sudo
# ------------------------------------------------------------------------------
echo ""
echo "==> Step 4: Checking pip availability..."
export PATH="$HOME/.local/bin:$PATH"

if ! python3 -m pip --version &>/dev/null; then
    echo "pip not found via 'python3 -m pip'. Attempting ensurepip..."
    python3 -m ensurepip --user --break-system-packages 2>/dev/null || python3 -m ensurepip --user 2>/dev/null || true
fi

if ! python3 -m pip --version &>/dev/null; then
    echo "Bootstrapping pip via get-pip.py (user-level)..."
    GET_PIP_TMP="/tmp/get-pip-$$.py"
    if command -v curl &>/dev/null; then
        curl -sS https://bootstrap.pypa.io/get-pip.py -o "$GET_PIP_TMP"
    elif command -v wget &>/dev/null; then
        wget -qO "$GET_PIP_TMP" https://bootstrap.pypa.io/get-pip.py
    else
        echo "Error: Neither curl nor wget is available to download get-pip.py." >&2
        exit 1
    fi
    # Invoke get-pip.py with --user and --break-system-packages for PEP 668 environments
    if ! python3 "$GET_PIP_TMP" --user --break-system-packages; then
        echo "Retrying get-pip.py without --break-system-packages..."
        python3 "$GET_PIP_TMP" --user || true
    fi
    rm -f "$GET_PIP_TMP"
fi

if ! python3 -m pip --version &>/dev/null; then
    echo "Error: Unable to bootstrap pip for current user without sudo." >&2
    exit 1
fi
echo "pip is ready: $(python3 -m pip --version 2>&1)"

# ------------------------------------------------------------------------------
# 5. Install Python dependencies from requirements.txt
# ------------------------------------------------------------------------------
echo ""
echo "==> Step 5: Installing dependencies from requirements.txt..."
if [ ! -f "requirements.txt" ]; then
    echo "Error: requirements.txt not found in $PROJECT_ROOT" >&2
    exit 1
fi

# Attempt user-level installation with --break-system-packages for PEP 668 compatibility
if ! python3 -m pip install --user --break-system-packages -r requirements.txt; then
    echo "Retrying dependency installation without --break-system-packages (for older pip versions)..."
    if ! python3 -m pip install --user -r requirements.txt; then
        echo "Error: Dependency installation from requirements.txt failed." >&2
        exit 1
    fi
fi

echo "Verifying google-cloud-pubsub import..."
if ! python3 -c "from google.cloud import pubsub_v1; print('Pub/Sub library OK')" 2>&1; then
    echo "Error: Verification of google-cloud-pubsub library failed." >&2
    exit 1
fi

# ------------------------------------------------------------------------------
# 6. Check / Create Pub/Sub Topic
# ------------------------------------------------------------------------------
PUBSUB_TOPIC="cloudqueue-jobs"
echo ""
echo "==> Step 6: Checking Pub/Sub topic '$PUBSUB_TOPIC'..."
if gcloud pubsub topics describe "$PUBSUB_TOPIC" --project="$PROJECT_ID" &>/dev/null; then
    echo "Pub/Sub topic '$PUBSUB_TOPIC' already exists."
else
    echo "Topic '$PUBSUB_TOPIC' not found. Creating..."
    if ! gcloud pubsub topics create "$PUBSUB_TOPIC" --project="$PROJECT_ID"; then
        echo "Error: Failed to create Pub/Sub topic '$PUBSUB_TOPIC'." >&2
        exit 1
    fi
    echo "Topic '$PUBSUB_TOPIC' created successfully."
fi

# ------------------------------------------------------------------------------
# 7. Check / Create Pub/Sub Subscription (shared by all workers)
# ------------------------------------------------------------------------------
PUBSUB_SUBSCRIPTION="cloudqueue-worker-sub"
echo ""
echo "==> Step 7: Checking Pub/Sub subscription '$PUBSUB_SUBSCRIPTION'..."
if gcloud pubsub subscriptions describe "$PUBSUB_SUBSCRIPTION" --project="$PROJECT_ID" &>/dev/null; then
    echo "Pub/Sub subscription '$PUBSUB_SUBSCRIPTION' already exists."
else
    echo "Subscription '$PUBSUB_SUBSCRIPTION' not found. Creating..."
    if ! gcloud pubsub subscriptions create "$PUBSUB_SUBSCRIPTION" \
        --topic="$PUBSUB_TOPIC" \
        --project="$PROJECT_ID"; then
        echo "Error: Failed to create Pub/Sub subscription '$PUBSUB_SUBSCRIPTION'." >&2
        exit 1
    fi
    echo "Subscription '$PUBSUB_SUBSCRIPTION' created successfully."
fi

# ------------------------------------------------------------------------------
# 8. Configure CloudQueue Environment (.env)
# ------------------------------------------------------------------------------
echo ""
echo "==> Step 8: Configuring CloudQueue environment (.env)..."
ENV_FILE="$PROJECT_ROOT/.env"
cat <<EOF > "$ENV_FILE"
CLOUDQUEUE_MODE=gcp
CLOUDQUEUE_ENV=development
DATABASE_PATH=database/cloudqueue.db
GOOGLE_CLOUD_PROJECT=$PROJECT_ID
PUBSUB_TOPIC=$PUBSUB_TOPIC
PUBSUB_SUBSCRIPTION=$PUBSUB_SUBSCRIPTION
PORT=5000
EOF
echo "Configuration written to .env for GCP mode."

# ------------------------------------------------------------------------------
# 9. Initialize SQLite Database Schema
# ------------------------------------------------------------------------------
echo ""
echo "==> Step 9: Initializing SQLite database schema..."
python3 -c "from app import init_db; init_db()"
python3 -c "
import sqlite3
conn = sqlite3.connect('database/cloudqueue.db')
cursor = conn.cursor()
cursor.execute(\"SELECT name FROM sqlite_master WHERE type='table'\")
tables = {r[0] for r in cursor.fetchall()}
conn.close()
required = {'jobs', 'experiments', 'experiment_jobs'}
if not required.issubset(tables):
    raise RuntimeError(f'Missing required tables: {required - tables}')
print('Database verified with tables: ' + ', '.join(sorted(list(required))))
"

# ------------------------------------------------------------------------------
# 10. Configure GCP Firewall Rule
# ------------------------------------------------------------------------------
echo ""
echo "==> Step 10: Configuring GCP firewall rule..."
FIREWALL_SCRIPT="$PROJECT_ROOT/gcp/setup_firewall.sh"
if [ -f "$FIREWALL_SCRIPT" ]; then
    chmod +x "$FIREWALL_SCRIPT" 2>/dev/null || true
    /bin/bash "$FIREWALL_SCRIPT"
else
    echo "Warning: Firewall script not found at $FIREWALL_SCRIPT" >&2
fi

# ------------------------------------------------------------------------------
# 11. Configure Automated Database Backup Cron Job
# ------------------------------------------------------------------------------
echo ""
echo "==> Step 11: Installing database backup cron job..."
CRON_SETUP_SCRIPT="$PROJECT_ROOT/gcp/setup_backup_cron.sh"
if [ -f "$CRON_SETUP_SCRIPT" ]; then
    chmod +x "$CRON_SETUP_SCRIPT" 2>/dev/null || true
    /bin/bash "$CRON_SETUP_SCRIPT"
else
    echo "Warning: Backup cron setup script not found at $CRON_SETUP_SCRIPT" >&2
fi

# ------------------------------------------------------------------------------
# 12. Final Setup Verification & Safe Summary
# ------------------------------------------------------------------------------
echo ""
echo "==> Step 12: Performing final checks..."
python3 --version
python3 -c "from google.cloud import pubsub_v1; print('Pub/Sub library OK')"

echo ""
echo "======================================================"
echo "CloudQueue setup complete."
echo ""
echo "Mode: GCP"
echo "Project: $PROJECT_ID"
echo "Topic: $PUBSUB_TOPIC"
echo "Subscription: $PUBSUB_SUBSCRIPTION"
echo "Database: database/cloudqueue.db"
echo "Port: 5000"
echo "Firewall: Rule 'cloudqueue-allow-5000' configured (TCP 5000, 0.0.0.0/0)"
echo "Installed Backup Cron Schedule: */5 * * * * (runs every 5 minutes)"
if command -v crontab &>/dev/null; then
    CRON_LINE="$(crontab -l 2>/dev/null | grep "backup_db.sh" || true)"
    if [ -n "$CRON_LINE" ]; then
        echo "Active Crontab: $CRON_LINE"
    fi
fi
echo "======================================================"
