import os


def _load_env_file():
    """Loads key-value pairs from .env if present without overriding existing env vars."""
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

# Core system mode ('local' or 'gcp')
CLOUDQUEUE_MODE = os.environ.get("CLOUDQUEUE_MODE", "local").strip().lower()
CLOUDQUEUE_ENV = os.environ.get("CLOUDQUEUE_ENV", "development").strip().lower()
DATABASE_PATH = os.environ.get("DATABASE_PATH", "database/cloudqueue.db").strip()

# Google Cloud Pub/Sub configuration (used in GCP mode)
GOOGLE_CLOUD_PROJECT = os.environ.get("GOOGLE_CLOUD_PROJECT", "").strip()
PUBSUB_TOPIC = os.environ.get("PUBSUB_TOPIC", "cloudqueue-jobs").strip()
PUBSUB_SUBSCRIPTION = os.environ.get("PUBSUB_SUBSCRIPTION", "cloudqueue-worker-sub").strip()

PORT = int(os.environ.get("PORT", 5000))


def is_gcp_mode() -> bool:
    return CLOUDQUEUE_MODE == "gcp"


def is_local_mode() -> bool:
    return CLOUDQUEUE_MODE != "gcp"
