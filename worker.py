import json
import base64
import os
import random
import smtplib
import time
import uuid
from datetime import datetime, timezone
from datetime import timedelta
from email.message import EmailMessage
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from celery import Celery
from celery.exceptions import Retry
from celery.signals import task_failure
from sqlalchemy.orm import Session

from .config import (
    DLQ_QUEUE_NAME,
    DLQ_MAX_ITEMS,
    DLQ_PRUNE_INTERVAL_SECONDS,
    DLQ_REPLAY_BATCH_SIZE,
    DLQ_REPLAY_DRY_RUN,
    DLQ_REPLAY_LOCK_TTL_SECONDS,
    DLQ_REPLAY_INTERVAL_SECONDS,
    CB_FAILURE_THRESHOLD,
    CB_OPEN_SECONDS,
    CHANNEL_RATE_LIMIT_PER_MINUTE,
    CHANNEL_RATE_LIMIT_WINDOW_SECONDS,
    CELERY_TASK_MAX_RETRIES,
    CELERY_TASK_RETRY_BACKOFF,
    CELERY_TASK_RETRY_BACKOFF_MAX,
    CELERY_TASK_RETRY_JITTER,
    CELERY_TASK_SOFT_TIME_LIMIT,
    CELERY_TASK_TIME_LIMIT,
    METRICS_SNAPSHOT_INTERVAL_SECONDS,
    INCIDENT_STALE_MINUTES,
    OPS_ENDPOINT_MAX_LIMIT,
    REDIS_URL,
    METRICS_KEY,
    METRICS_TTL_SECONDS,
    APP_BASE_URL,
    VOICE_PRERECORDED_AUDIO_URL,
    VOICE_TWIML_MODE,
    VOICE_PROVIDER,
    TWILIO_ACCOUNT_SID,
    TWILIO_AUTH_TOKEN,
    TWILIO_FROM_NUMBER,
    SIGNALWIRE_SPACE_URL,
    SIGNALWIRE_PROJECT_ID,
    SIGNALWIRE_API_TOKEN,
    SIGNALWIRE_FROM_NUMBER,
    TELEGRAM_BOT_TOKEN,
    TELEGRAM_LOGS_CHAT_ID,
    SMTP_HOST,
    SMTP_PORT,
    SMTP_USER,
    SMTP_PASSWORD,
    SMTP_FROM_EMAIL,
    SMTP_USE_TLS,
    SMTP_USE_SSL,
    SMTP_TIMEOUT_SECONDS,
    OASIS_RADAR_PULL_ENABLED,
    OASIS_RADAR_PULL_FAILURES_KEY,
    OASIS_RADAR_PULL_FAILURE_ALERT_THRESHOLD,
    OASIS_RADAR_PULL_INTERVAL_SECONDS,
    OASIS_RADAR_PULL_LIMIT,
    OASIS_RADAR_LOOKBACK_SECONDS,
    queue_name,
    prefixed_redis_key,
)
from .database import SessionLocal
from .models import (
    AuditAction,
    AuditLog,
    Contact,
    GroupMember,
    Incident,
    IncidentStatus,
    Notification,
    NotificationChannel,
    NotificationStatus,
    Rule,
)
from .redis_client import redis_conn
from .observability import init_observability, notify_event, notify_exception

DLQ_REDIS_KEY = prefixed_redis_key(DLQ_QUEUE_NAME)
DLQ_REPLAY_LOCK_KEY = prefixed_redis_key(f"{DLQ_QUEUE_NAME}:replay:lock")
METRICS_REDIS_KEY = prefixed_redis_key(METRICS_KEY)
DLQ_REPLAY_REPORT_KEY = prefixed_redis_key(f"{METRICS_KEY}:dlq_replay_last")

celery_app = Celery("event_saas", broker=REDIS_URL, backend=REDIS_URL)
init_observability()

celery_app.conf.update(
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    timezone="UTC",
    enable_utc=True,
    task_acks_late=True,
    task_reject_on_worker_lost=True,
    worker_prefetch_multiplier=1,
    task_routes={
        "dispatcher": {"queue": queue_name("dispatch")},
        "voice_worker": {"queue": queue_name("voice")},
        "telegram_worker": {"queue": queue_name("telegram")},
        "email_worker": {"queue": queue_name("email")},
        "replay_dlq": {"queue": queue_name(DLQ_QUEUE_NAME)},
        "prune_dlq": {"queue": queue_name(DLQ_QUEUE_NAME)},
    },
    beat_schedule={
        "queue-metrics-snapshot": {
            "task": "queue_metrics_snapshot",
            "schedule": METRICS_SNAPSHOT_INTERVAL_SECONDS,
        },
        "dlq-replay": {
            "task": "replay_dlq",
            "schedule": DLQ_REPLAY_INTERVAL_SECONDS,
        },
        "dlq-prune": {
            "task": "prune_dlq",
            "schedule": DLQ_PRUNE_INTERVAL_SECONDS,
        },
        "oasis-radar-pull": {
            "task": "oasis_radar_pull_worker",
            "schedule": OASIS_RADAR_PULL_INTERVAL_SECONDS,
        },
    },
)


def _notification_channel_value(channel: NotificationChannel | str) -> str:
    return channel.value if isinstance(channel, NotificationChannel) else str(channel)


def _voice_twiml_url(notification_id: str, trace_id: str | None = None) -> str:
    mode = (VOICE_TWIML_MODE or "dynamic").strip().lower()
    query = f"?trace_id={trace_id}" if trace_id else ""
    if mode == "prerecorded" and VOICE_PRERECORDED_AUDIO_URL:
        return (
            f"{APP_BASE_URL}/dispatch/voice/twiml/prerecorded/{notification_id}{query}"
        )
    return f"{APP_BASE_URL}/dispatch/voice/twiml/{notification_id}{query}"


def _render_notification_template(
    rule: Rule | None,
    incident: Incident,
    channel: str,
) -> str:
    templates = getattr(rule, "notification_templates_json", None) or {}
    template = None
    if isinstance(templates, dict):
        template = templates.get(channel.upper()) or templates.get(channel.lower())

    if not template:
        template = "[{severity}] {title} | source={source} | incident={incident_id}"

    values = {
        "severity": incident.severity,
        "title": incident.title,
        "source": incident.source,
        "message": incident.message or "",
        "incident_id": str(incident.id),
        "runbook_url": getattr(rule, "runbook_url", "") or "",
    }
    try:
        return str(template).format(**values)
    except Exception:
        return str(template)


def _get_channel_retry_policy(
    notification_id: str,
    channel_name: str,
) -> dict[str, int | bool]:
    defaults = {
        "max_retries": CELERY_TASK_MAX_RETRIES,
        "backoff_seconds": CELERY_TASK_RETRY_BACKOFF,
        "backoff_max_seconds": CELERY_TASK_RETRY_BACKOFF_MAX,
        "jitter": CELERY_TASK_RETRY_JITTER,
    }
    with SessionLocal() as db:
        notification = (
            db.query(Notification).filter(Notification.id == notification_id).first()
        )
        if not notification:
            return defaults
        incident = (
            db.query(Incident).filter(Incident.id == notification.incident_id).first()
        )
        if not incident or not incident.matched_rule_id:
            return defaults
        rule = db.query(Rule).filter(Rule.id == incident.matched_rule_id).first()
        if not rule or not rule.channel_retry_policy_json:
            return defaults

        policy = rule.channel_retry_policy_json
        if not isinstance(policy, dict):
            return defaults
        channel_policy = policy.get(channel_name.upper()) or policy.get(
            channel_name.lower()
        )
        if not isinstance(channel_policy, dict):
            return defaults

        merged = dict(defaults)
        for key in ("max_retries", "backoff_seconds", "backoff_max_seconds", "jitter"):
            if key in channel_policy:
                merged[key] = channel_policy[key]
        return merged


def _call_provider_voice(
    endpoint: str,
    account_id: str,
    auth_token: str,
    from_number: str,
    contact_phone: str,
    twiml_url: str,
    provider_label: str,
) -> str:
    if not all([account_id, auth_token, from_number]):
        raise RuntimeError(f"{provider_label} credentials are not configured")

    payload = urlencode(
        {
            "To": contact_phone,
            "From": from_number,
            "Url": twiml_url,
            "Method": "POST",
        }
    ).encode("utf-8")
    basic_auth = base64.b64encode(f"{account_id}:{auth_token}".encode("utf-8")).decode(
        "ascii"
    )
    request = Request(
        endpoint,
        data=payload,
        method="POST",
        headers={
            "Authorization": f"Basic {basic_auth}",
            "Content-Type": "application/x-www-form-urlencoded",
        },
    )

    with urlopen(request, timeout=15) as response:
        body = response.read().decode("utf-8")
        parsed = json.loads(body)
        sid = parsed.get("sid")
        if not sid:
            raise RuntimeError(f"{provider_label} response did not include call sid")
        return str(sid)


def _call_twilio_voice(contact_phone: str, twiml_url: str) -> str:
    endpoint = (
        f"https://api.twilio.com/2010-04-01/Accounts/{TWILIO_ACCOUNT_SID}/Calls.json"
    )
    return _call_provider_voice(
        endpoint=endpoint,
        account_id=TWILIO_ACCOUNT_SID,
        auth_token=TWILIO_AUTH_TOKEN,
        from_number=TWILIO_FROM_NUMBER,
        contact_phone=contact_phone,
        twiml_url=twiml_url,
        provider_label="Twilio",
    )


def _call_signalwire_voice(contact_phone: str, twiml_url: str) -> str:
    if not SIGNALWIRE_SPACE_URL:
        raise RuntimeError("SignalWire space URL is not configured")

    space = SIGNALWIRE_SPACE_URL.strip().rstrip("/")
    if not space.startswith("http://") and not space.startswith("https://"):
        space = f"https://{space}"

    endpoint = (
        f"{space}/api/laml/2010-04-01/Accounts/{SIGNALWIRE_PROJECT_ID}/Calls.json"
    )
    return _call_provider_voice(
        endpoint=endpoint,
        account_id=SIGNALWIRE_PROJECT_ID,
        auth_token=SIGNALWIRE_API_TOKEN,
        from_number=SIGNALWIRE_FROM_NUMBER,
        contact_phone=contact_phone,
        twiml_url=twiml_url,
        provider_label="SignalWire",
    )


def _telegram_target_chat_id(contact: Contact | None) -> str:
    if contact and contact.telegram_id:
        return str(contact.telegram_id)
    return str(TELEGRAM_LOGS_CHAT_ID)


def _send_telegram_via_bot_api(chat_id: str, text: str) -> str:
    token = TELEGRAM_BOT_TOKEN.strip()
    if not token:
        raise RuntimeError("Telegram bot token is not configured")

    endpoint = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = json.dumps(
        {
            "chat_id": str(chat_id),
            "text": text,
            "disable_web_page_preview": True,
        }
    ).encode("utf-8")
    request = Request(
        endpoint,
        data=payload,
        method="POST",
        headers={"Content-Type": "application/json"},
    )

    try:
        with urlopen(request, timeout=15) as response:
            body = response.read().decode("utf-8")
    except HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(
            f"Telegram sendMessage failed: HTTP {exc.code} - {body}"
        ) from exc
    except URLError as exc:
        raise RuntimeError(f"Telegram sendMessage failed: {exc.reason}") from exc

    parsed = json.loads(body)
    if not parsed.get("ok"):
        raise RuntimeError(
            f"Telegram sendMessage failed: {parsed.get('description', 'unknown error')}"
        )

    result = parsed.get("result") or {}
    message_id = result.get("message_id")
    if message_id is None:
        raise RuntimeError("Telegram sendMessage response did not include message_id")
    return str(message_id)


def _smtp_is_configured() -> bool:
    return bool(SMTP_HOST.strip())


def _send_email_via_smtp(
    *,
    to_email: str,
    subject: str,
    body: str,
) -> str:
    if not _smtp_is_configured():
        raise RuntimeError("SMTP is not configured (missing SMTP_HOST)")

    from_email = (SMTP_FROM_EMAIL or SMTP_USER).strip()
    if not from_email:
        raise RuntimeError(
            "SMTP sender is not configured (set SMTP_FROM_EMAIL or SMTP_USER)"
        )

    message = EmailMessage()
    message["From"] = from_email
    message["To"] = to_email
    message["Subject"] = subject
    message.set_content(body)

    smtp_cls = smtplib.SMTP_SSL if SMTP_USE_SSL else smtplib.SMTP
    with smtp_cls(SMTP_HOST, SMTP_PORT, timeout=SMTP_TIMEOUT_SECONDS) as smtp:
        if not SMTP_USE_SSL and SMTP_USE_TLS:
            smtp.starttls()
        if SMTP_USER:
            smtp.login(SMTP_USER, SMTP_PASSWORD)
        smtp.send_message(message)

    return f"smtp:{subject}"


def _pull_oasis_radar_impl(limit: int | None = None) -> dict:
    if not OASIS_RADAR_PULL_ENABLED:
        return {"status": "skipped", "reason": "disabled"}

    admin_token = os.environ.get("SERIEMA_ADMIN_TOKEN", "")
    if not admin_token:
        return {"status": "skipped", "reason": "missing_admin_token"}

    requested_limit = OASIS_RADAR_PULL_LIMIT if limit is None else int(limit)
    endpoint = (
        f"{APP_BASE_URL}/integrations/oasis-radar/pull"
        f"?lookback_seconds={OASIS_RADAR_LOOKBACK_SECONDS}&limit={requested_limit}"
    )
    request = Request(
        endpoint,
        method="POST",
        headers={"X-Admin-Token": admin_token},
    )
    try:
        with urlopen(request, timeout=20) as response:
            payload = response.read().decode("utf-8")
            parsed = json.loads(payload)
        redis_conn.delete(prefixed_redis_key(OASIS_RADAR_PULL_FAILURES_KEY))
        notify_event("seriema.oasis_radar.pull_worker", parsed)
        return parsed
    except Exception as exc:
        failures = int(
            redis_conn.incr(prefixed_redis_key(OASIS_RADAR_PULL_FAILURES_KEY))
        )
        if failures == 1:
            redis_conn.expire(
                prefixed_redis_key(OASIS_RADAR_PULL_FAILURES_KEY),
                max(300, OASIS_RADAR_PULL_INTERVAL_SECONDS * 10),
            )
        level = (
            "critical"
            if failures >= OASIS_RADAR_PULL_FAILURE_ALERT_THRESHOLD
            else "error"
        )
        notify_event(
            "seriema.oasis_radar.pull_worker_failed",
            {
                "failures": failures,
                "threshold": OASIS_RADAR_PULL_FAILURE_ALERT_THRESHOLD,
                "error": str(exc),
            },
            level=level,
        )
        raise


def _dispatch_voice_provider(
    db: Session, notification: Notification, trace_id: str | None = None
) -> str | None:
    provider = (VOICE_PROVIDER or "mock").strip().lower()
    if provider not in {"twilio", "signalwire"}:
        return None

    contact = db.query(Contact).filter(Contact.id == notification.contact_id).first()
    if not contact or not contact.phone:
        raise RuntimeError("Contact phone is not available for voice dispatch")

    if provider == "twilio":
        return _call_twilio_voice(
            str(contact.phone),
            _voice_twiml_url(str(notification.id), trace_id=trace_id),
        )
    return _call_signalwire_voice(
        str(contact.phone), _voice_twiml_url(str(notification.id), trace_id=trace_id)
    )


def _notification_is_terminal(notification: Notification) -> bool:
    return notification.status in {
        NotificationStatus.SENT,
        NotificationStatus.DELIVERED,
        NotificationStatus.ANSWERED_VOICE,
    }


def _channel_name(channel: NotificationChannel | str) -> str:
    return _notification_channel_value(channel).upper()


def _channel_circuit_open_key(channel: NotificationChannel | str) -> str:
    return prefixed_redis_key(f"cb:{_channel_name(channel).lower()}:open")


def _channel_circuit_failure_key(channel: NotificationChannel | str) -> str:
    return prefixed_redis_key(f"cb:{_channel_name(channel).lower()}:failures")


def _channel_rate_limit_key(
    channel: NotificationChannel | str,
    window_seconds: int | None = None,
    now_epoch: int | None = None,
) -> str:
    window = (
        CHANNEL_RATE_LIMIT_WINDOW_SECONDS
        if window_seconds is None
        else max(1, int(window_seconds))
    )
    epoch = int(time.time()) if now_epoch is None else int(now_epoch)
    bucket = epoch // window
    return prefixed_redis_key(f"rl:{_channel_name(channel).lower()}:{bucket}")


def _channel_is_circuit_open(channel: NotificationChannel | str) -> bool:
    return bool(redis_conn.exists(_channel_circuit_open_key(channel)))


def _reset_channel_circuit_state(channel: NotificationChannel | str) -> None:
    redis_conn.delete(_channel_circuit_failure_key(channel))


def _open_channel_circuit(channel: NotificationChannel | str) -> None:
    channel_name = _channel_name(channel)
    pipe = redis_conn.pipeline()
    pipe.set(_channel_circuit_open_key(channel_name), "1", ex=CB_OPEN_SECONDS)
    pipe.delete(_channel_circuit_failure_key(channel_name))
    pipe.execute()


def _record_channel_failure(
    channel: NotificationChannel | str,
) -> dict[str, int | bool | str]:
    channel_name = _channel_name(channel)
    failures = int(redis_conn.incr(_channel_circuit_failure_key(channel_name)))
    opened = failures >= CB_FAILURE_THRESHOLD
    if opened:
        _open_channel_circuit(channel_name)
        notify_event(
            "seriema.circuit_breaker.opened",
            {
                "channel": channel_name,
                "failures": failures,
                "threshold": CB_FAILURE_THRESHOLD,
            },
            level="critical",
        )
    _touch_metrics(
        {
            f"cb_failures:{channel_name.lower()}": failures,
            f"cb_open:{channel_name.lower()}": int(opened),
        }
    )
    return {"channel": channel_name, "failures": failures, "opened": opened}


def _channel_rate_limit_exceeded(
    channel: NotificationChannel | str,
    limit_per_minute: int | None = None,
    window_seconds: int | None = None,
    now_epoch: int | None = None,
) -> dict[str, int | bool | str]:
    limit = (
        CHANNEL_RATE_LIMIT_PER_MINUTE
        if limit_per_minute is None
        else max(1, int(limit_per_minute))
    )
    window = (
        CHANNEL_RATE_LIMIT_WINDOW_SECONDS
        if window_seconds is None
        else max(1, int(window_seconds))
    )
    key = _channel_rate_limit_key(channel, window_seconds=window, now_epoch=now_epoch)
    count = redis_conn.incr(key)
    if count == 1:
        redis_conn.expire(key, window)
    exceeded = count > limit
    _touch_metrics(
        {
            f"rate_limit_count:{_channel_name(channel).lower()}": count,
            f"rate_limit_exceeded:{_channel_name(channel).lower()}": int(exceeded),
        }
    )
    return {
        "channel": _channel_name(channel),
        "limit": limit,
        "count": count,
        "window_seconds": window,
        "exceeded": exceeded,
    }


def _metric_timestamp() -> str:
    return datetime.now(timezone.utc).isoformat()


def _stale_cutoff(now: datetime | None = None) -> datetime:
    current = now or datetime.now(timezone.utc)
    return current - timedelta(minutes=INCIDENT_STALE_MINUTES)


def _touch_metrics(fields: dict[str, int | str]) -> None:
    if not fields:
        return

    pipe = redis_conn.pipeline()
    pipe.hset(METRICS_REDIS_KEY, mapping=fields)
    pipe.hset(METRICS_REDIS_KEY, "updated_at", _metric_timestamp())
    pipe.expire(METRICS_REDIS_KEY, METRICS_TTL_SECONDS)
    pipe.execute()


def _increment_metric_counter(prefix: str, channel: str, amount: int = 1) -> None:
    pipe = redis_conn.pipeline()
    pipe.hincrby(METRICS_REDIS_KEY, f"{prefix}:{channel}", amount)
    pipe.hset(METRICS_REDIS_KEY, "updated_at", _metric_timestamp())
    pipe.expire(METRICS_REDIS_KEY, METRICS_TTL_SECONDS)
    pipe.execute()


def _refresh_dlq_metric() -> None:
    _touch_metrics({"dlq_size": redis_conn.llen(DLQ_REDIS_KEY)})


def _record_periodic_task_heartbeat(
    task_name: str, started_monotonic: float, status: str
) -> None:
    duration_ms = max(0, int((time.monotonic() - started_monotonic) * 1000))
    _touch_metrics(
        {
            f"last_run_at:{task_name}": _metric_timestamp(),
            f"last_status:{task_name}": status,
            f"duration_ms:{task_name}": duration_ms,
        }
    )


def _write_dlq_replay_report(
    *,
    status: str,
    started_at: str | None,
    finished_at: str | None,
    requested_limit: int | None,
    effective_limit: int | None,
    replayed: int,
    remaining: int,
    dry_run: bool,
    locked: bool,
    candidates_count: int | None = None,
    error_message: str | None = None,
) -> None:
    redis_conn.hset(
        DLQ_REPLAY_REPORT_KEY,
        mapping={
            "status": status,
            "started_at": started_at or "",
            "finished_at": finished_at or "",
            "requested_limit": "" if requested_limit is None else int(requested_limit),
            "effective_limit": "" if effective_limit is None else int(effective_limit),
            "replayed": int(replayed),
            "remaining": int(remaining),
            "dry_run": int(bool(dry_run)),
            "locked": int(bool(locked)),
            "candidates_count": (
                "" if candidates_count is None else int(candidates_count)
            ),
            "error_message": error_message or "",
        },
    )
    redis_conn.expire(DLQ_REPLAY_REPORT_KEY, METRICS_TTL_SECONDS)


def get_dlq_replay_report() -> dict[str, str | int | bool]:
    raw = redis_conn.hgetall(DLQ_REPLAY_REPORT_KEY)
    report: dict[str, str | int | bool] = {}
    for key, value in raw.items():
        field = key.decode("utf-8") if isinstance(key, bytes) else str(key)
        decoded = value.decode("utf-8") if isinstance(value, bytes) else value
        if field in {
            "requested_limit",
            "effective_limit",
            "replayed",
            "remaining",
            "dry_run",
            "locked",
            "candidates_count",
        }:
            try:
                report[field] = int(decoded)
                continue
            except (TypeError, ValueError):
                pass
        report[field] = decoded
    return report


def _bound_dlq_max_items(max_items: int | None = None) -> int:
    requested = DLQ_MAX_ITEMS if max_items is None else int(max_items)
    return max(1, requested)


def _trim_dlq_to_limit(max_items: int | None = None) -> int:
    limit = _bound_dlq_max_items(max_items)
    current_size = redis_conn.llen(DLQ_REDIS_KEY)
    if current_size <= limit:
        return 0

    redis_conn.ltrim(DLQ_REDIS_KEY, -limit, -1)
    _refresh_dlq_metric()
    return current_size - limit


def _snapshot_queue_backlog() -> dict[str, int | str]:
    snapshot = {
        "queue_backlog:dispatch": redis_conn.llen(queue_name("dispatch")),
        "queue_backlog:voice": redis_conn.llen(queue_name("voice")),
        "queue_backlog:telegram": redis_conn.llen(queue_name("telegram")),
        "queue_backlog:email": redis_conn.llen(queue_name("email")),
        "queue_backlog:escalation": redis_conn.llen(queue_name("escalation")),
        "queue_backlog:dlq": redis_conn.llen(DLQ_REDIS_KEY),
    }
    _touch_metrics(snapshot)
    return snapshot


def _bound_replay_limit(limit: int | None) -> int:
    requested = DLQ_REPLAY_BATCH_SIZE if limit is None else int(limit)
    return max(1, min(requested, OPS_ENDPOINT_MAX_LIMIT))


def _acquire_dlq_replay_lock() -> str | None:
    token = uuid.uuid4().hex
    acquired = redis_conn.set(
        DLQ_REPLAY_LOCK_KEY,
        token,
        nx=True,
        ex=DLQ_REPLAY_LOCK_TTL_SECONDS,
    )
    return token if acquired else None


def _release_dlq_replay_lock(token: str | None) -> None:
    if not token:
        return
    current = redis_conn.get(DLQ_REPLAY_LOCK_KEY)
    if current is not None and current.decode() == token:
        redis_conn.delete(DLQ_REPLAY_LOCK_KEY)


def _get_or_create_notification(
    db: Session,
    incident_id: str,
    contact_id: str,
    channel: NotificationChannel,
) -> tuple[Notification, bool]:
    notification = (
        db.query(Notification)
        .filter(
            Notification.incident_id == incident_id,
            Notification.contact_id == contact_id,
            Notification.channel == channel,
        )
        .first()
    )
    if notification:
        return notification, False

    notification = Notification(
        incident_id=incident_id,
        contact_id=contact_id,
        channel=channel,
    )
    db.add(notification)
    db.commit()
    db.refresh(notification)
    return notification, True


def _queue_channel_send(
    notification_id: str, trace_id: str, channel: NotificationChannel
) -> None:
    task_map = {
        NotificationChannel.VOICE: send_voice_call,
        NotificationChannel.TELEGRAM: send_telegram_message,
        NotificationChannel.EMAIL: send_email_message,
    }
    task = task_map.get(channel)
    if not task:
        return

    task.apply_async(
        args=(notification_id, trace_id),
        queue=queue_name(channel.value.lower()),
    )


def _dlq_payload(
    task_name: str,
    args: tuple,
    kwargs: dict,
    exception: Exception | str,
) -> dict:
    trace_id = None
    notification_id = None
    incident_id = None

    if len(args) > 1:
        trace_id = args[1]
    if len(args) > 0:
        if task_name == "escalation_worker":
            incident_id = args[0]
        else:
            notification_id = args[0]

    trace_id = kwargs.get("trace_id", trace_id)
    notification_id = kwargs.get("notification_id", notification_id)
    incident_id = kwargs.get("incident_id", incident_id)

    return {
        "task_name": task_name,
        "args": list(args),
        "kwargs": kwargs,
        "trace_id": trace_id,
        "notification_id": notification_id,
        "incident_id": incident_id,
        "error": str(exception),
        "failed_at": uuid.uuid4().hex,
    }


def _push_dlq_entry(
    task_name: str, args: tuple, kwargs: dict, exception: Exception | str
) -> dict:
    payload = _dlq_payload(task_name, args, kwargs, exception)
    redis_conn.rpush(DLQ_REDIS_KEY, json.dumps(payload, sort_keys=True))
    _trim_dlq_to_limit()
    _refresh_dlq_metric()
    return payload


def _record_final_failure(
    task_name: str,
    payload: dict,
) -> None:
    with SessionLocal() as db:
        trace_id = payload.get("trace_id") or str(uuid.uuid4())
        notification_id = payload.get("notification_id")
        incident_id = payload.get("incident_id")
        channel_name = None

        if notification_id:
            notification = (
                db.query(Notification)
                .filter(Notification.id == notification_id)
                .with_for_update()
                .first()
            )
            if notification and not _notification_is_terminal(notification):
                notification.status = NotificationStatus.FAILED
                notification.error_message = payload.get("error")
                channel_name = _notification_channel_value(notification.channel)
            if notification and not incident_id:
                incident_id = notification.incident_id
        elif task_name == "escalation_worker":
            _increment_metric_counter("tasks_failed_by_channel", "ESCALATION")

        if channel_name in {"VOICE", "TELEGRAM", "EMAIL"}:
            _increment_metric_counter("tasks_failed_by_channel", channel_name)
            _record_channel_failure(channel_name)

        db.add(
            AuditLog(
                trace_id=trace_id,
                incident_id=incident_id,
                action=AuditAction.FAILED,
                details_json={
                    "task_name": task_name,
                    "notification_id": notification_id,
                    "error": payload.get("error"),
                },
            )
        )
        db.commit()
        notify_event(
            "seriema.worker.task_final_failure",
            {
                "task_name": task_name,
                "trace_id": trace_id,
                "notification_id": str(notification_id) if notification_id else None,
                "incident_id": str(incident_id) if incident_id else None,
                "error": payload.get("error"),
            },
            level="error",
            trace_id=trace_id,
        )


def _handle_task_final_failure(
    task_name: str, args: tuple, kwargs: dict, exception: Exception
) -> None:
    payload = _push_dlq_entry(task_name, args, kwargs, exception)
    _record_final_failure(task_name, payload)


@task_failure.connect(weak=False)
def _on_task_failure(
    sender=None,
    task_id=None,
    exception=None,
    args=None,
    kwargs=None,
    traceback=None,
    einfo=None,
    **extra,
):
    task_name = getattr(sender, "name", None) or str(sender or "")
    if task_name not in {
        "voice_worker",
        "telegram_worker",
        "email_worker",
        "escalation_worker",
    }:
        return

    # For autoretry tasks, only send to DLQ when retries are exhausted.
    if isinstance(exception, Retry):
        return
    request_retries = int(getattr(getattr(sender, "request", None), "retries", 0))
    max_retries = CELERY_TASK_MAX_RETRIES
    if task_name in {"voice_worker", "telegram_worker", "email_worker"} and args:
        channel_name = task_name.replace("_worker", "").upper()
        try:
            policy = _get_channel_retry_policy(str(args[0]), channel_name)
            max_retries = int(policy["max_retries"])
        except Exception:
            max_retries = CELERY_TASK_MAX_RETRIES
    if max_retries is not None and request_retries < max_retries:
        return

    _handle_task_final_failure(
        task_name,
        tuple(args or ()),
        dict(kwargs or {}),
        exception or Exception("task failed"),
    )


def _mark_notification_sent(
    db: Session,
    notification_id: str,
    trace_id: str,
    provider_channel: str,
    provider_id: str | None = None,
    message_preview: str | None = None,
    runbook_url: str | None = None,
) -> bool:
    notification = (
        db.query(Notification)
        .filter(Notification.id == notification_id)
        .with_for_update()
        .first()
    )
    if not notification:
        raise LookupError(f"notification not found: {notification_id}")
    if _notification_is_terminal(notification):
        return False

    notification.status = NotificationStatus.SENT
    notification.external_provider_id = provider_id
    _increment_metric_counter("tasks_sent_by_channel", provider_channel)
    audit = AuditLog(
        trace_id=trace_id,
        incident_id=notification.incident_id,
        action=AuditAction.NOTIFICATION_SENT,
        details_json={
            "channel": provider_channel,
            "notification_id": notification_id,
            "provider_id": provider_id,
            "trace_id": trace_id,
            "message_preview": message_preview,
            "runbook_url": runbook_url,
        },
    )
    db.add(audit)
    db.commit()
    _reset_channel_circuit_state(provider_channel)
    return True


def _mark_notification_failed(
    db: Session,
    notification_id: str,
    trace_id: str,
    provider_channel: str,
    error_message: str,
    message_preview: str | None = None,
    runbook_url: str | None = None,
) -> bool:
    notification = (
        db.query(Notification)
        .filter(Notification.id == notification_id)
        .with_for_update()
        .first()
    )
    if not notification:
        raise LookupError(f"notification not found: {notification_id}")
    if _notification_is_terminal(notification):
        return False

    notification.status = NotificationStatus.FAILED
    notification.error_message = error_message
    _increment_metric_counter("tasks_failed_by_channel", provider_channel)
    _record_channel_failure(provider_channel)
    db.add(
        AuditLog(
            trace_id=trace_id,
            incident_id=notification.incident_id,
            action=AuditAction.FAILED,
            details_json={
                "channel": provider_channel,
                "notification_id": notification_id,
                "trace_id": trace_id,
                "error": error_message,
                "message_preview": message_preview,
                "runbook_url": runbook_url,
            },
        )
    )
    db.commit()
    return True


def _send_notification_channel_impl(
    notification_id: str,
    trace_id: str,
    provider_channel: NotificationChannel | str,
):
    channel_name = _channel_name(provider_channel)
    with SessionLocal() as db:
        try:
            notification = (
                db.query(Notification)
                .filter(Notification.id == notification_id)
                .with_for_update()
                .first()
            )
            if not notification:
                raise LookupError(f"notification not found: {notification_id}")
            if _notification_is_terminal(notification):
                return {
                    "status": "already_terminal",
                    "channel": channel_name,
                    "notification_id": notification_id,
                    "trace_id": trace_id,
                }

            incident = (
                db.query(Incident)
                .filter(Incident.id == notification.incident_id)
                .first()
            )
            rule = (
                db.query(Rule).filter(Rule.id == incident.matched_rule_id).first()
                if incident and incident.matched_rule_id
                else None
            )
            message_preview = (
                _render_notification_template(rule, incident, channel_name)
                if incident
                else None
            )

            if channel_name in {
                "VOICE",
                "TELEGRAM",
                "EMAIL",
            } and _channel_is_circuit_open(channel_name):
                return {
                    "status": "skipped_circuit",
                    "channel": channel_name,
                    "notification_id": notification_id,
                    "trace_id": trace_id,
                }

            rate_limit = None
            if channel_name in {"VOICE", "TELEGRAM", "EMAIL"}:
                rate_limit = _channel_rate_limit_exceeded(channel_name)
                if rate_limit["exceeded"]:
                    db.add(
                        AuditLog(
                            trace_id=trace_id,
                            incident_id=notification.incident_id,
                            action=AuditAction.FAILED,
                            details_json={
                                "channel": channel_name,
                                "notification_id": notification_id,
                                "reason": "rate_limited",
                                "limit": rate_limit["limit"],
                                "count": rate_limit["count"],
                                "window_seconds": rate_limit["window_seconds"],
                            },
                        )
                    )
                    db.commit()

                    # We record rate-limited deliveries as FAILED because the task did not emit the message.
                    _increment_metric_counter("tasks_failed_by_channel", channel_name)
                    return {
                        "status": "skipped_rate_limit",
                        "channel": channel_name,
                        "notification_id": notification_id,
                        "trace_id": trace_id,
                        "rate_limit": rate_limit,
                    }

            contact = (
                db.query(Contact).filter(Contact.id == notification.contact_id).first()
            )
            provider_id = None
            runbook_url = getattr(rule, "runbook_url", None)
            if channel_name == "VOICE":
                provider_id = _dispatch_voice_provider(
                    db, notification, trace_id=trace_id
                )
            elif channel_name == "TELEGRAM":
                error_message = None
                if not TELEGRAM_BOT_TOKEN.strip():
                    error_message = "Telegram bot token is not configured"
                if error_message:
                    _mark_notification_failed(
                        db,
                        notification_id,
                        trace_id,
                        channel_name,
                        error_message,
                        message_preview=message_preview,
                        runbook_url=runbook_url,
                    )
                    return {
                        "status": "failed",
                        "channel": channel_name,
                        "notification_id": notification_id,
                        "trace_id": trace_id,
                        "error": error_message,
                    }
                provider_id = _send_telegram_via_bot_api(
                    _telegram_target_chat_id(contact),
                    message_preview or "Alerta sem conteúdo.",
                )
            elif channel_name == "EMAIL":
                error_message = None
                if not contact or not contact.email:
                    error_message = "Contact email is not available for email dispatch"
                elif not _smtp_is_configured():
                    error_message = "SMTP is not configured (missing SMTP_HOST)"
                    notify_event(
                        "seriema.worker.email_not_configured",
                        {
                            "notification_id": notification_id,
                            "incident_id": str(notification.incident_id),
                        },
                        level="warning",
                        trace_id=trace_id,
                    )
                if error_message:
                    _mark_notification_failed(
                        db,
                        notification_id,
                        trace_id,
                        channel_name,
                        error_message,
                        message_preview=message_preview,
                        runbook_url=runbook_url,
                    )
                    return {
                        "status": "failed",
                        "channel": channel_name,
                        "notification_id": notification_id,
                        "trace_id": trace_id,
                        "error": error_message,
                    }
                runbook_url = (
                    runbook_url
                    or f"{APP_BASE_URL}/incidents/{notification.incident_id}"
                )
                body = (message_preview or "Alerta sem conteúdo.") + (
                    f"\n\nLink: {runbook_url}"
                )
                provider_id = _send_email_via_smtp(
                    to_email=str(contact.email),
                    subject=(incident.title if incident else "Incident alert"),
                    body=body,
                )

            sent = _mark_notification_sent(
                db,
                notification_id,
                trace_id,
                channel_name,
                provider_id=provider_id,
                message_preview=message_preview,
                runbook_url=runbook_url,
            )
            if sent:
                return {
                    "status": "sent",
                    "channel": channel_name,
                    "notification_id": notification_id,
                    "trace_id": trace_id,
                }
            return {
                "status": "already_terminal",
                "channel": channel_name,
                "notification_id": notification_id,
                "trace_id": trace_id,
            }
        except Exception as exc:
            notify_exception(
                exc,
                {
                    "stage": "send_notification_channel",
                    "notification_id": notification_id,
                    "channel": channel_name,
                },
                trace_id=trace_id,
            )
            raise


@celery_app.task(name="dispatcher")
def dispatch_incident(incident_id: str, incoming_trace_id: str):
    with SessionLocal() as db:
        incident = db.query(Incident).filter(Incident.id == incident_id).first()
        if not incident:
            return

        rule = db.query(Rule).filter(Rule.id == incident.matched_rule_id).first()
        if not rule:
            return

        group_members = (
            db.query(GroupMember)
            .filter(GroupMember.group_id == rule.recipient_group_id)
            .all()
        )
        channels = rule.channels

        created_notifications = []
        for member in group_members:
            for channel in channels:
                try:
                    notification_channel = NotificationChannel[str(channel).upper()]
                except KeyError:
                    continue

                notif, created = _get_or_create_notification(
                    db,
                    str(incident.id),
                    str(member.contact_id),
                    notification_channel,
                )
                if created or not _notification_is_terminal(notif):
                    created_notifications.append(notif)

        for notif in created_notifications:
            channel_value = _notification_channel_value(notif.channel)
            message_preview = _render_notification_template(
                rule, incident, channel_value
            )
            audit = AuditLog(
                trace_id=incoming_trace_id,
                incident_id=incident.id,
                action=AuditAction.TASK_QUEUED,
                details_json={
                    "channel": channel_value,
                    "notification_id": str(notif.id),
                    "trace_id": incoming_trace_id,
                    "message_preview": message_preview,
                    "runbook_url": getattr(rule, "runbook_url", None),
                },
            )
            db.add(audit)
            db.commit()

            if notif.channel == NotificationChannel.VOICE:
                _queue_channel_send(
                    str(notif.id), incoming_trace_id, NotificationChannel.VOICE
                )
            elif notif.channel == NotificationChannel.TELEGRAM:
                _queue_channel_send(
                    str(notif.id), incoming_trace_id, NotificationChannel.TELEGRAM
                )
            elif notif.channel == NotificationChannel.EMAIL:
                _queue_channel_send(
                    str(notif.id), incoming_trace_id, NotificationChannel.EMAIL
                )
        notify_event(
            "seriema.worker.dispatch_completed",
            {
                "incident_id": str(incident.id),
                "trace_id": incoming_trace_id,
                "notifications_queued": len(created_notifications),
            },
            trace_id=incoming_trace_id,
        )


@celery_app.task(
    name="voice_worker",
    bind=True,
    soft_time_limit=CELERY_TASK_SOFT_TIME_LIMIT,
    time_limit=CELERY_TASK_TIME_LIMIT,
)
def send_voice_call(self, notification_id: str, trace_id: str):
    try:
        return _send_notification_channel_impl(
            notification_id, trace_id, NotificationChannel.VOICE
        )
    except Exception as exc:
        policy = _get_channel_retry_policy(notification_id, "VOICE")
        retries = int(getattr(self.request, "retries", 0))
        max_retries = int(policy["max_retries"])
        if retries >= max_retries:
            raise
        countdown = min(
            int(policy["backoff_seconds"]) * (2**retries),
            int(policy["backoff_max_seconds"]),
        )
        if bool(policy["jitter"]):
            countdown += random.randint(0, max(1, countdown // 3))
        raise self.retry(exc=exc, countdown=max(1, countdown), max_retries=max_retries)


def _send_voice_call_impl(notification_id: str, trace_id: str):
    return _send_notification_channel_impl(
        notification_id, trace_id, NotificationChannel.VOICE
    )


@celery_app.task(
    name="telegram_worker",
    bind=True,
    soft_time_limit=CELERY_TASK_SOFT_TIME_LIMIT,
    time_limit=CELERY_TASK_TIME_LIMIT,
)
def send_telegram_message(self, notification_id: str, trace_id: str):
    try:
        return _send_notification_channel_impl(
            notification_id, trace_id, NotificationChannel.TELEGRAM
        )
    except Exception as exc:
        policy = _get_channel_retry_policy(notification_id, "TELEGRAM")
        retries = int(getattr(self.request, "retries", 0))
        max_retries = int(policy["max_retries"])
        if retries >= max_retries:
            raise
        countdown = min(
            int(policy["backoff_seconds"]) * (2**retries),
            int(policy["backoff_max_seconds"]),
        )
        if bool(policy["jitter"]):
            countdown += random.randint(0, max(1, countdown // 3))
        raise self.retry(exc=exc, countdown=max(1, countdown), max_retries=max_retries)


def _send_telegram_message_impl(notification_id: str, trace_id: str):
    return _send_notification_channel_impl(
        notification_id, trace_id, NotificationChannel.TELEGRAM
    )


@celery_app.task(
    name="email_worker",
    bind=True,
    soft_time_limit=CELERY_TASK_SOFT_TIME_LIMIT,
    time_limit=CELERY_TASK_TIME_LIMIT,
)
def send_email_message(self, notification_id: str, trace_id: str):
    try:
        return _send_notification_channel_impl(
            notification_id, trace_id, NotificationChannel.EMAIL
        )
    except Exception as exc:
        policy = _get_channel_retry_policy(notification_id, "EMAIL")
        retries = int(getattr(self.request, "retries", 0))
        max_retries = int(policy["max_retries"])
        if retries >= max_retries:
            raise
        countdown = min(
            int(policy["backoff_seconds"]) * (2**retries),
            int(policy["backoff_max_seconds"]),
        )
        if bool(policy["jitter"]):
            countdown += random.randint(0, max(1, countdown // 3))
        raise self.retry(exc=exc, countdown=max(1, countdown), max_retries=max_retries)


def _send_email_message_impl(notification_id: str, trace_id: str):
    return _send_notification_channel_impl(
        notification_id, trace_id, NotificationChannel.EMAIL
    )


@celery_app.task(name="oasis_radar_pull_worker")
def oasis_radar_pull_worker(limit: int | None = None):
    return _pull_oasis_radar_impl(limit=limit)


# desabilitado temporariamente — sem oncall configurado
# Mantido no código para futura reativação, mas sem registro no Celery ao iniciar.
def handle_escalation(incident_id: str, trace_id: str):
    return _handle_escalation_impl(incident_id, trace_id)


def _handle_escalation_impl(incident_id: str, trace_id: str):
    with SessionLocal() as db:
        incident = (
            db.query(Incident)
            .filter(Incident.id == incident_id)
            .with_for_update()
            .first()
        )
        if not incident:
            raise LookupError(f"incident not found: {incident_id}")
        if incident.status != IncidentStatus.OPEN:
            return True

        incident.status = IncidentStatus.ESCALATED
        audit = AuditLog(
            trace_id=trace_id,
            incident_id=incident.id,
            action=AuditAction.ESCALATED,
            details_json={"reason": "ACK deadline expired without acknowledgment."},
        )
        db.add(audit)
        db.commit()

        rule = db.query(Rule).filter(Rule.id == incident.matched_rule_id).first()
        if not rule or not rule.fallback_policy_json:
            return True

        policy = rule.fallback_policy_json
        invalid_field = None
        diagnostic: dict[str, str] = {
            "policy_type": type(policy).__name__,
        }

        if not isinstance(policy, dict):
            invalid_field = "policy"
        else:
            raw_group_id = policy.get("escalation_group_id") or policy.get(
                "escalation_group"
            )
            raw_channels = policy.get("channels", ["VOICE"])
            diagnostic["escalation_group_id_type"] = (
                type(raw_group_id).__name__ if raw_group_id is not None else "NoneType"
            )
            diagnostic["channels_type"] = type(raw_channels).__name__

            if isinstance(raw_group_id, uuid.UUID):
                escalation_group_id = raw_group_id
            elif isinstance(raw_group_id, str):
                try:
                    escalation_group_id = uuid.UUID(raw_group_id)
                except (TypeError, ValueError):
                    escalation_group_id = None
                    invalid_field = "escalation_group_id"
            else:
                escalation_group_id = None
                invalid_field = "escalation_group_id"

            fallback_channels = None
            if invalid_field is None:
                if not isinstance(raw_channels, (list, tuple, set)):
                    invalid_field = "channels"
                else:
                    fallback_channels = []
                    for channel in raw_channels:
                        if not isinstance(channel, str):
                            invalid_field = "channels"
                            break
                        fallback_channels.append(channel)

        if invalid_field is not None:
            db.add(
                AuditLog(
                    trace_id=trace_id,
                    incident_id=incident.id,
                    action=AuditAction.FAILED,
                    details_json={
                        "reason": "invalid_fallback_policy",
                        "diagnostic": {
                            "invalid_field": invalid_field,
                            **diagnostic,
                        },
                    },
                )
            )
            db.commit()
            return True

        members = (
            db.query(GroupMember)
            .filter(GroupMember.group_id == escalation_group_id)
            .all()
        )
        for member in members:
            for channel in fallback_channels:
                try:
                    notification_channel = NotificationChannel[str(channel).upper()]
                except KeyError:
                    continue

                notif, _ = _get_or_create_notification(
                    db,
                    str(incident.id),
                    str(member.contact_id),
                    notification_channel,
                )
                if _notification_is_terminal(notif):
                    continue
                audit_2 = AuditLog(
                    trace_id=trace_id,
                    incident_id=incident.id,
                    action=AuditAction.FALLBACK_TASK_QUEUED,
                    details_json={
                        "channel": notification_channel.value,
                        "notification_id": str(notif.id),
                    },
                )
                db.add(audit_2)
                db.commit()

                _queue_channel_send(str(notif.id), trace_id, notification_channel)
        return True


# desabilitado temporariamente — sem oncall configurado
# Mantido no código para futura reativação, mas sem registro no Celery ao iniciar.
def stale_incident_sweeper():
    db: Session = SessionLocal()
    swept = 0
    started_monotonic = time.monotonic()
    try:
        try:
            cutoff = _stale_cutoff()
            stale_incidents = (
                db.query(Incident)
                .filter(
                    Incident.status == IncidentStatus.OPEN,
                    Incident.created_at < cutoff,
                )
                .all()
            )

            for incident in stale_incidents:
                if incident.status != IncidentStatus.OPEN:
                    continue

                incident.status = IncidentStatus.ESCALATED
                db.add(
                    AuditLog(
                        trace_id=str(uuid.uuid4()),
                        incident_id=incident.id,
                        action=AuditAction.ESCALATED,
                        details_json={"reason": "stale_sweeper"},
                    )
                )
                swept += 1

            if swept:
                db.commit()
            result = {"swept": swept, "cutoff": cutoff.isoformat()}
            _record_periodic_task_heartbeat(
                "stale_incident_sweeper", started_monotonic, "ok"
            )
            return result
        except Exception:
            _record_periodic_task_heartbeat(
                "stale_incident_sweeper", started_monotonic, "error"
            )
            raise
    finally:
        db.close()


def _replay_entry(entry: dict) -> bool:
    task_name = entry.get("task_name")
    args = entry.get("args", [])

    if task_name == "voice_worker":
        result = _send_voice_call_impl(args[0], args[1])
        return isinstance(result, dict) and result.get("status") in {
            "sent",
            "already_terminal",
        }
    if task_name == "telegram_worker":
        result = _send_telegram_message_impl(args[0], args[1])
        return isinstance(result, dict) and result.get("status") in {
            "sent",
            "already_terminal",
        }
    if task_name == "email_worker":
        result = _send_email_message_impl(args[0], args[1])
        return isinstance(result, dict) and result.get("status") in {
            "sent",
            "already_terminal",
        }
    if task_name == "escalation_worker":
        return _handle_escalation_impl(args[0], args[1])
    return False


@celery_app.task(name="replay_dlq")
def replay_dlq(limit: int | None = None):
    batch_limit = _bound_replay_limit(limit)
    requested_limit = DLQ_REPLAY_BATCH_SIZE if limit is None else int(limit)
    started_at = _metric_timestamp()
    candidates_count = 0
    lock_token = _acquire_dlq_replay_lock()
    if not lock_token:
        remaining = redis_conn.llen(DLQ_REDIS_KEY)
        _write_dlq_replay_report(
            status="locked",
            started_at=started_at,
            finished_at=_metric_timestamp(),
            requested_limit=requested_limit,
            effective_limit=batch_limit,
            replayed=0,
            remaining=remaining,
            dry_run=DLQ_REPLAY_DRY_RUN,
            locked=True,
            candidates_count=0,
            error_message="",
        )
        return {
            "status": "locked",
            "replayed": 0,
            "remaining": remaining,
            "dry_run": DLQ_REPLAY_DRY_RUN,
            "limit": batch_limit,
            "candidates": None,
        }

    try:
        try:
            raw_entries = redis_conn.lrange(DLQ_REDIS_KEY, 0, batch_limit - 1)
            replayed = 0
            candidates = []

            for raw_entry in raw_entries:
                try:
                    entry = json.loads(raw_entry)
                    candidates.append(entry)
                    if DLQ_REPLAY_DRY_RUN:
                        continue

                    error_str = str(entry.get("error", "")).lower()
                    if (
                        "validationerror" in error_str
                        or "bad request" in error_str
                        or "400" in error_str
                    ):
                        redis_conn.lrem(DLQ_REDIS_KEY, 1, raw_entry)
                        _refresh_dlq_metric()
                        continue

                    if _replay_entry(entry):
                        redis_conn.lrem(DLQ_REDIS_KEY, 1, raw_entry)
                        replayed += 1
                        _refresh_dlq_metric()
                except Exception:
                    continue

            candidates_count = len(candidates)
            _refresh_dlq_metric()
            result = {
                "status": "completed",
                "replayed": replayed,
                "remaining": redis_conn.llen(DLQ_REDIS_KEY),
                "dry_run": DLQ_REPLAY_DRY_RUN,
                "limit": batch_limit,
                "candidates": candidates if DLQ_REPLAY_DRY_RUN else None,
            }
            if DLQ_REPLAY_DRY_RUN:
                _touch_metrics(
                    {
                        "dlq_dry_run_candidates": len(candidates),
                        "dlq_dry_run_limit": batch_limit,
                    }
                )
            _write_dlq_replay_report(
                status="completed",
                started_at=started_at,
                finished_at=_metric_timestamp(),
                requested_limit=requested_limit,
                effective_limit=batch_limit,
                replayed=replayed,
                remaining=result["remaining"],
                dry_run=DLQ_REPLAY_DRY_RUN,
                locked=False,
                candidates_count=candidates_count,
                error_message="",
            )
            return result
        except Exception as exc:
            remaining = redis_conn.llen(DLQ_REDIS_KEY)
            _write_dlq_replay_report(
                status="error",
                started_at=started_at,
                finished_at=_metric_timestamp(),
                requested_limit=requested_limit,
                effective_limit=batch_limit,
                replayed=0,
                remaining=remaining,
                dry_run=DLQ_REPLAY_DRY_RUN,
                locked=False,
                candidates_count=candidates_count,
                error_message=str(exc),
            )
            raise exc
    finally:
        _release_dlq_replay_lock(lock_token)


@celery_app.task(name="queue_metrics_snapshot")
def queue_metrics_snapshot():
    started_monotonic = time.monotonic()
    try:
        result = _snapshot_queue_backlog()
        _record_periodic_task_heartbeat(
            "queue_metrics_snapshot", started_monotonic, "ok"
        )
        return result
    except Exception:
        _record_periodic_task_heartbeat(
            "queue_metrics_snapshot", started_monotonic, "error"
        )
        raise


@celery_app.task(name="prune_dlq")
def prune_dlq(max_items: int | None = None):
    started_monotonic = time.monotonic()
    try:
        limit = _bound_dlq_max_items(max_items)
        current_size = redis_conn.llen(DLQ_REDIS_KEY)
        if current_size <= limit:
            _refresh_dlq_metric()
            result = {"removed": 0, "remaining": current_size, "max_items": limit}
        else:
            removed = current_size - limit
            redis_conn.ltrim(DLQ_REDIS_KEY, -limit, -1)
            remaining = redis_conn.llen(DLQ_REDIS_KEY)
            _refresh_dlq_metric()
            result = {"removed": removed, "remaining": remaining, "max_items": limit}

        _record_periodic_task_heartbeat("prune_dlq", started_monotonic, "ok")
        return result
    except Exception:
        _record_periodic_task_heartbeat("prune_dlq", started_monotonic, "error")
        raise
