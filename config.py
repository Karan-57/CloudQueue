import os

# ==============================================================================
# CloudQueue Configuration
# ==============================================================================

def _load_env_file():
    """
    Loads key-value pairs from .env if present into os.environ,
    without overriding any explicitly set environment variables.
    """
    env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")
    if os.path.isfile(env_path):
        try:
            with open(env_path, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line or line.startswith("#") or "=" not in line:
                        continue
                    key, _, val = line.partition("=")
                    key = key.strip()
                    val = val.strip().strip("'\"")
                    if key and key not in os.environ:
                        os.environ[key] = val
        except Exception:
            pass

_load_env_file()

# Operation Mode: 'local' (SQLite-based queue) or 'gcp' (Google Cloud Pub/Sub)
CLOUDQUEUE_MODE = os.environ.get("CLOUDQUEUE_MODE", "local").strip().lower()

# General Environment: 'development', 'production', 'test'
CLOUDQUEUE_ENV = os.environ.get("CLOUDQUEUE_ENV", "development").strip().lower()

# Database Path (Used for job results, experiments metadata, and local queue)
DATABASE_PATH = os.environ.get("DATABASE_PATH", "database/cloudqueue.db").strip()

# Google Cloud Platform Pub/Sub Settings (Used when CLOUDQUEUE_MODE='gcp')
GOOGLE_CLOUD_PROJECT = os.environ.get("GOOGLE_CLOUD_PROJECT", "").strip()
PUBSUB_TOPIC = os.environ.get("PUBSUB_TOPIC", "cloudqueue-jobs").strip()
PUBSUB_SUBSCRIPTION = os.environ.get("PUBSUB_SUBSCRIPTION", "cloudqueue-worker-sub").strip()

# Web Server Port
PORT = int(os.environ.get("PORT", 5000))


def is_gcp_mode() -> bool:
    """Returns True if the system is configured to run in GCP Pub/Sub mode."""
    return CLOUDQUEUE_MODE == "gcp"


def is_local_mode() -> bool:
    """Returns True if the system is running in local SQLite queue mode."""
    return CLOUDQUEUE_MODE != "gcp"
