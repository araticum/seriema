import os
from urllib.parse import urlparse, urlunparse


def _build_redis_url_with_db(redis_url: str, redis_db: int) -> str:
    parsed = urlparse(redis_url)
    normalized_path = f"/{redis_db}"
    return urlunparse(
        (
            parsed.scheme,
            parsed.netloc,
            normalized_path,
            parsed.params,
            parsed.query,
            parsed.fragment,
        )
    )


DB_SCHEMA = os.getenv("SERIEMA_DB_SCHEMA", "seriema")

DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://postgres:postgres@localhost:5432/eventsaas",
)

REDIS_DB = int(os.getenv("SERIEMA_REDIS_DB", "5"))
RAW_REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
REDIS_URL = _build_redis_url_with_db(RAW_REDIS_URL, REDIS_DB)

REDIS_KEY_PREFIX = os.getenv("SERIEMA_REDIS_KEY_PREFIX", "seriema")
QUEUE_PREFIX = os.getenv("SERIEMA_QUEUE_PREFIX", "queue:seriema")

APP_BASE_URL = os.getenv("APP_BASE_URL", "https://api.event-saas.com")
VOICE_PRERECORDED_AUDIO_URL = os.getenv("VOICE_PRERECORDED_AUDIO_URL", "")
VOICE_TWIML_MODE = os.getenv("VOICE_TWIML_MODE", "dynamic").lower()
VOICE_PROVIDER = os.getenv("VOICE_PROVIDER", "mock").lower()
TWILIO_ACCOUNT_SID = os.getenv("TWILIO_ACCOUNT_SID", "")
TWILIO_AUTH_TOKEN = os.getenv("TWILIO_AUTH_TOKEN", "")
TWILIO_FROM_NUMBER = os.getenv("TWILIO_FROM_NUMBER", "")
SIGNALWIRE_SPACE_URL = os.getenv("SIGNALWIRE_SPACE_URL", "")
SIGNALWIRE_PROJECT_ID = os.getenv("SIGNALWIRE_PROJECT_ID", "")
SIGNALWIRE_API_TOKEN = os.getenv("SIGNALWIRE_API_TOKEN", "")
SIGNALWIRE_FROM_NUMBER = os.getenv("SIGNALWIRE_FROM_NUMBER", "")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_LOGS_CHAT_ID = os.getenv("TELEGRAM_LOGS_CHAT_ID", "-1003623240453")
RESEND_API_KEY = os.getenv("RESEND_API_KEY", "")
RESEND_FROM_EMAIL = os.getenv("RESEND_FROM_EMAIL", "logs@araticum.net")
RESEND_TO_EMAIL = os.getenv("RESEND_TO_EMAIL", "adm@araticum.net")
# Deprecated: legacy SMTP config kept temporarily for backwards compatibility.
SMTP_HOST = os.getenv("SMTP_HOST", "")
SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
SMTP_USER = os.getenv("SMTP_USER", "")
SMTP_PASSWORD = os.getenv("SMTP_PASSWORD", "")
SMTP_FROM_EMAIL = os.getenv("SMTP_FROM_EMAIL", "")
SMTP_USE_TLS = os.getenv("SMTP_USE_TLS", "true").lower() == "true"
SMTP_USE_SSL = os.getenv("SMTP_USE_SSL", "false").lower() == "true"
SMTP_TIMEOUT_SECONDS = int(os.getenv("SMTP_TIMEOUT_SECONDS", "15"))

CELERY_TASK_MAX_RETRIES = int(os.getenv("SERIEMA_TASK_MAX_RETRIES", "5"))
CELERY_TASK_RETRY_BACKOFF = int(os.getenv("SERIEMA_TASK_RETRY_BACKOFF", "2"))
CELERY_TASK_RETRY_BACKOFF_MAX = int(os.getenv("SERIEMA_TASK_RETRY_BACKOFF_MAX", "60"))
CELERY_TASK_RETRY_JITTER = (
    os.getenv("SERIEMA_TASK_RETRY_JITTER", "true").lower() == "true"
)
CELERY_TASK_SOFT_TIME_LIMIT = int(os.getenv("SERIEMA_TASK_SOFT_TIME_LIMIT", "30"))
CELERY_TASK_TIME_LIMIT = int(os.getenv("SERIEMA_TASK_TIME_LIMIT", "45"))
DLQ_QUEUE_NAME = os.getenv("SERIEMA_DLQ_QUEUE_NAME", "dlq")
DLQ_REPLAY_BATCH_SIZE = int(os.getenv("SERIEMA_DLQ_REPLAY_BATCH_SIZE", "10"))
DLQ_MAX_ITEMS = int(os.getenv("SERIEMA_DLQ_MAX_ITEMS", "1000"))
METRICS_KEY = os.getenv("SERIEMA_METRICS_KEY", "metrics:ops")
METRICS_TTL_SECONDS = int(os.getenv("SERIEMA_METRICS_TTL_SECONDS", "120"))
METRICS_SNAPSHOT_INTERVAL_SECONDS = int(
    os.getenv("SERIEMA_METRICS_SNAPSHOT_INTERVAL_SECONDS", "60")
)
DLQ_REPLAY_INTERVAL_SECONDS = int(
    os.getenv("SERIEMA_DLQ_REPLAY_INTERVAL_SECONDS", "120")
)
DLQ_PRUNE_INTERVAL_SECONDS = int(os.getenv("SERIEMA_DLQ_PRUNE_INTERVAL_SECONDS", "300"))
OPS_ENDPOINT_MAX_LIMIT = int(os.getenv("SERIEMA_OPS_MAX_LIMIT", "100"))
VOICE_WEBHOOK_MAX_SKEW_SECONDS = int(os.getenv("VOICE_WEBHOOK_MAX_AGE_SECONDS", "300"))
DLQ_REPLAY_DRY_RUN = os.getenv("SERIEMA_DLQ_REPLAY_DRY_RUN", "false").lower() == "true"
DLQ_REPLAY_LOCK_TTL_SECONDS = int(
    os.getenv("SERIEMA_DLQ_REPLAY_LOCK_TTL_SECONDS", "60")
)
ALERT_DLQ_WARN = int(os.getenv("SERIEMA_ALERT_DLQ_WARN", "10"))
ALERT_DLQ_CRIT = int(os.getenv("SERIEMA_ALERT_DLQ_CRIT", "50"))
ALERT_QUEUE_WARN = int(os.getenv("SERIEMA_ALERT_QUEUE_WARN", "25"))
ALERT_ACK_RATE_WARN = float(os.getenv("SERIEMA_ALERT_ACK_RATE_WARN", "0.9"))
CB_FAILURE_THRESHOLD = int(os.getenv("SERIEMA_CB_FAILURE_THRESHOLD", "5"))
CB_OPEN_SECONDS = int(os.getenv("SERIEMA_CB_OPEN_SECONDS", "60"))
CHANNEL_RATE_LIMIT_PER_MINUTE = int(
    os.getenv("SERIEMA_CHANNEL_RATE_LIMIT_PER_MINUTE", "60")
)
CHANNEL_RATE_LIMIT_WINDOW_SECONDS = int(
    os.getenv("SERIEMA_CHANNEL_RATE_LIMIT_WINDOW_SECONDS", "60")
)
INCIDENT_STALE_MINUTES = int(os.getenv("SERIEMA_INCIDENT_STALE_MINUTES", "30"))
INCIDENT_STALE_SWEEP_INTERVAL_SECONDS = int(
    os.getenv("SERIEMA_INCIDENT_STALE_SWEEP_INTERVAL_SECONDS", "120")
)

SENTRY_DSN = os.getenv("SENTRY_DSN", "")
SENTRY_ENVIRONMENT = os.getenv("SENTRY_ENVIRONMENT", "production")
SENTRY_RELEASE = os.getenv("SENTRY_RELEASE", "")
SENTRY_WEBHOOK_SIGNING_SECRET = os.getenv("SENTRY_WEBHOOK_SIGNING_SECRET", "")
LANGFUSE_PUBLIC_KEY = os.getenv("LANGFUSE_PUBLIC_KEY", "")
LANGFUSE_SECRET_KEY = os.getenv("LANGFUSE_SECRET_KEY", "")
LANGFUSE_HOST = os.getenv("LANGFUSE_HOST", "https://cloud.langfuse.com")
LANGFUSE_WEBHOOK_SECRET = os.getenv("LANGFUSE_WEBHOOK_SECRET", "")
TELEGRAM_NOTIFICATION_DEDUPE_WINDOW_SECONDS = int(
    os.getenv("TELEGRAM_NOTIFICATION_DEDUPE_WINDOW_SECONDS", "300")
)
SERIEMA_PROMETHEUS_ENABLED = (
    os.getenv("SERIEMA_PROMETHEUS_ENABLED", "false").lower() == "true"
)

OASIS_RADAR_ENABLED = os.getenv("OASIS_RADAR_ENABLED", "false").lower() == "true"
OASIS_RADAR_LOKI_URL = os.getenv("OASIS_RADAR_LOKI_URL", "http://oasis-radar-loki:3100")
OASIS_RADAR_LOGQL_QUERY = os.getenv(
    "OASIS_RADAR_LOGQL_QUERY",
    '{compose_service=~".+"}',
)
OASIS_RADAR_LOOKBACK_SECONDS = int(os.getenv("OASIS_RADAR_LOOKBACK_SECONDS", "300"))
OASIS_RADAR_PULL_LIMIT = int(os.getenv("OASIS_RADAR_PULL_LIMIT", "100"))
OASIS_RADAR_PULL_ENABLED = (
    os.getenv("OASIS_RADAR_PULL_ENABLED", "false").lower() == "true"
)
OASIS_RADAR_PULL_INTERVAL_SECONDS = int(
    os.getenv("OASIS_RADAR_PULL_INTERVAL_SECONDS", "60")
)
OASIS_RADAR_CURSOR_KEY = os.getenv(
    "OASIS_RADAR_CURSOR_KEY", "integrations:oasis_radar:last_timestamp_ns"
)
OASIS_RADAR_PULL_FAILURES_KEY = os.getenv(
    "OASIS_RADAR_PULL_FAILURES_KEY", "integrations:oasis_radar:pull_failures"
)
OASIS_RADAR_PULL_FAILURE_ALERT_THRESHOLD = int(
    os.getenv("OASIS_RADAR_PULL_FAILURE_ALERT_THRESHOLD", "3")
)


def queue_name(suffix: str) -> str:
    normalized = suffix.strip(":")
    return f"{QUEUE_PREFIX}:{normalized}"


def prefixed_redis_key(key: str) -> str:
    if not key:
        return REDIS_KEY_PREFIX
    return f"{REDIS_KEY_PREFIX}:{key}"
