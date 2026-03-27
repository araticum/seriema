import json
import uuid
import hashlib
import hmac
import os
import time
import re
from urllib.parse import urlencode
from urllib.request import Request as UrlRequest, urlopen
from urllib.error import URLError, HTTPError
from datetime import datetime, timezone, timedelta
from typing import cast as typing_cast, Literal as typing_Literal
from fastapi import FastAPI, Depends, HTTPException, Request, Response, Query, Header
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError
from sqlalchemy import func, text
from . import models, schemas
from .database import get_db
from .redis_client import is_duplicate, redis_conn
from .engine import evaluate_rules
from .classifier import classify_log_line
from .worker import dispatch_incident, replay_dlq, celery_app, get_dlq_replay_report
from .observability import init_observability, notify_event, notify_exception
from .config import (
    APP_BASE_URL,
    ALERT_ACK_RATE_WARN,
    ALERT_DLQ_CRIT,
    ALERT_DLQ_WARN,
    ALERT_QUEUE_WARN,
    DLQ_QUEUE_NAME,
    METRICS_KEY,
    OPS_ENDPOINT_MAX_LIMIT,
    SERIEMA_PROMETHEUS_ENABLED,
    SIGNALWIRE_API_TOKEN,
    SIGNALWIRE_FROM_NUMBER,
    SIGNALWIRE_PROJECT_ID,
    SIGNALWIRE_SPACE_URL,
    SENTRY_DSN,
    SENTRY_WEBHOOK_SIGNING_SECRET,
    LANGFUSE_PUBLIC_KEY,
    LANGFUSE_SECRET_KEY,
    LANGFUSE_WEBHOOK_SECRET,
    TWILIO_ACCOUNT_SID,
    TWILIO_AUTH_TOKEN,
    TWILIO_FROM_NUMBER,
    VOICE_PROVIDER,
    VOICE_PRERECORDED_AUDIO_URL,
    VOICE_WEBHOOK_MAX_SKEW_SECONDS,
    OASIS_RADAR_ENABLED,
    OASIS_RADAR_LOKI_URL,
    OASIS_RADAR_LOGQL_QUERY,
    OASIS_RADAR_LOOKBACK_SECONDS,
    OASIS_RADAR_CURSOR_KEY,
    OASIS_RADAR_PULL_LIMIT,
    queue_name,
    prefixed_redis_key,
)

VOICE_WEBHOOK_SECRET = os.environ.get("VOICE_WEBHOOK_SECRET")

app = FastAPI(title="Event SaaS API", version="0.1.0")
init_observability()


@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException):
    return JSONResponse(
        status_code=exc.status_code,
        content={
            "type": "about:blank",
            "title": "HTTP Error",
            "status": exc.status_code,
            "detail": exc.detail,
        },
        headers={"Content-Type": "application/problem+json"},
    )


@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    normalized_errors = []
    for item in exc.errors():
        normalized = dict(item)
        ctx = normalized.get("ctx")
        if isinstance(ctx, dict):
            normalized["ctx"] = {key: str(value) for key, value in ctx.items()}
        normalized_errors.append(normalized)

    return JSONResponse(
        status_code=422,
        content={
            "type": "about:blank",
            "title": "Validation Error",
            "status": 422,
            # Keep FastAPI-compatible shape for existing tests/clients.
            "detail": normalized_errors,
            "errors": normalized_errors,
        },
        headers={"Content-Type": "application/problem+json"},
    )


@app.exception_handler(Exception)
async def unhandled_exception_handler(request: Request, exc: Exception):
    notify_exception(exc, {"path": request.url.path, "method": request.method})
    return JSONResponse(
        status_code=500,
        content={
            "type": "about:blank",
            "title": "Internal Server Error",
            "status": 500,
            "detail": "An unexpected error occurred.",
        },
        headers={"Content-Type": "application/problem+json"},
    )


if SERIEMA_PROMETHEUS_ENABLED:
    try:
        from prometheus_fastapi_instrumentator import Instrumentator

        Instrumentator().instrument(app).expose(app)
        notify_event(
            "seriema.prometheus.enabled",
            {"metrics_path": "/metrics"},
        )
    except Exception as exc:
        notify_exception(
            exc,
            {"stage": "prometheus_instrumentation"},
        )


def _serialize_incident(incident: models.Incident) -> schemas.IncidentResponse:
    return schemas.IncidentResponse(
        id=incident.id,
        external_event_id=incident.external_event_id,
        source=incident.source,
        severity=incident.severity,
        service=incident.service,
        title=incident.title,
        message=incident.message,
        payload_json=incident.payload_json,
        status=incident.status,
        matched_rule_id=incident.matched_rule_id,
        dedupe_key=incident.dedupe_key,
        created_at=incident.created_at,
        updated_at=incident.updated_at,
        acknowledged_at=incident.acknowledged_at,
        acknowledged_by=incident.acknowledged_by,
    )


def _serialize_audit(audit: models.AuditLog) -> schemas.AuditLogResponse:
    return schemas.AuditLogResponse(
        id=audit.id,
        trace_id=audit.trace_id,
        incident_id=audit.incident_id,
        action=audit.action,
        details_json=audit.details_json,
        created_at=audit.created_at,
    )


def _serialize_rule(rule: models.Rule) -> schemas.RuleResponse:
    return schemas.RuleResponse(
        id=rule.id,
        rule_name=rule.rule_name,
        condition_json=rule.condition_json,
        recipient_group_id=rule.recipient_group_id,
        channels=rule.channels,
        active=rule.active,
        priority=rule.priority,
        requires_ack=rule.requires_ack,
        ack_deadline=rule.ack_deadline,
        fallback_policy_json=rule.fallback_policy_json,
        dedupe_window_seconds=rule.dedupe_window_seconds,
        dedupe_fields_json=rule.dedupe_fields_json,
        notification_templates_json=rule.notification_templates_json,
        runbook_url=rule.runbook_url,
        channel_retry_policy_json=rule.channel_retry_policy_json,
    )


def _extract_dedupe_value(event_dict: dict[str, object], path: str) -> str:
    current: object = event_dict
    for part in [segment for segment in str(path).split(".") if segment]:
        if isinstance(current, dict):
            current = current.get(part)
        else:
            current = None
        if current is None:
            break
    if current is None:
        return ""
    if isinstance(current, (dict, list)):
        return json.dumps(current, sort_keys=True, ensure_ascii=True)
    return str(current)


def _build_rule_dedupe_key(
    event_dict: dict[str, object],
    matched_rule: models.Rule | None,
) -> str | None:
    if not matched_rule:
        return None
    fields = matched_rule.dedupe_fields_json or []
    if not isinstance(fields, list) or not fields:
        return None

    window = max(1, int(matched_rule.dedupe_window_seconds or 300))
    bucket = int(time.time()) // window
    basis = "|".join(
        [f"{field}={_extract_dedupe_value(event_dict, str(field))}" for field in fields]
    )
    digest = hashlib.sha256(
        f"{matched_rule.id}|{bucket}|{basis}".encode("utf-8")
    ).hexdigest()
    return f"rule:{matched_rule.id}:{digest}"


def _simulate_rule(
    rule: models.Rule, payload: dict[str, object]
) -> schemas.RuleSimulationResponse:
    reasons: list[str] = []
    for key, expected_value in rule.condition_json.items():
        actual_value = payload.get(key, None)
        if actual_value != expected_value:
            reasons.append(f"{key}: expected {expected_value!r}, got {actual_value!r}")

    matched = len(reasons) == 0
    return schemas.RuleSimulationResponse(
        rule_id=rule.id,
        rule_name=rule.rule_name,
        matched=matched,
        reasons=reasons,
        payload=payload,
        condition_json=rule.condition_json,
    )


def _get_or_create_trace_id(db: Session, incident_id: uuid.UUID) -> str:
    first_log = (
        db.query(models.AuditLog)
        .filter(models.AuditLog.incident_id == incident_id)
        .order_by(models.AuditLog.created_at.asc())
        .first()
    )
    if first_log:
        return first_log.trace_id
    return str(uuid.uuid4())


def _queue_length(queue_key: str) -> int:
    return int(redis_conn.llen(queue_key) or 0)


def _metric_value_from_redis(value):
    if isinstance(value, bytes):
        value = value.decode("utf-8")
    if isinstance(value, str) and re.fullmatch(r"[+-]?\d+", value):
        try:
            return int(value)
        except ValueError:
            return value
    return value


def _dependency_status(
    ok: bool, detail: str | None = None
) -> schemas.DependencyStatusDetail:
    return schemas.DependencyStatusDetail(
        status="ok" if ok else "down",
        detail=detail,
    )


def _severity_rank(severity: str) -> int:
    return {"ok": 0, "info": 1, "warn": 2, "critical": 3}.get(severity, 0)


def _overall_severity(alerts: list[schemas.OperationalAlert]) -> str:
    if not alerts:
        return "ok"
    return max(alerts, key=lambda alert: _severity_rank(alert.severity)).severity


def _require_admin_token(x_admin_token: str | None) -> None:
    configured_token = os.environ.get("SERIEMA_ADMIN_TOKEN")
    if not configured_token:
        return
    if not x_admin_token or not hmac.compare_digest(x_admin_token, configured_token):
        raise HTTPException(status_code=401, detail="Invalid admin token")


def _parse_dlq_entry(raw_entry: bytes | str) -> dict:
    if isinstance(raw_entry, bytes):
        raw_entry = raw_entry.decode("utf-8")
    try:
        return json.loads(raw_entry)
    except json.JSONDecodeError:
        return {
            "task_name": "unknown",
            "error": "invalid_json",
            "raw": raw_entry,
        }


def _dlq_items(limit: int) -> tuple[int, list[schemas.DLQPreviewItem]]:
    total_items = int(redis_conn.llen(prefixed_redis_key(DLQ_QUEUE_NAME)) or 0)
    raw_entries = redis_conn.lrange(
        prefixed_redis_key(DLQ_QUEUE_NAME), 0, max(limit, 0) - 1
    )
    items: list[schemas.DLQPreviewItem] = []
    for raw_entry in raw_entries:
        payload = _parse_dlq_entry(raw_entry)
        items.append(
            schemas.DLQPreviewItem(
                task_name=str(payload.get("task_name", "unknown")),
                trace_id=payload.get("trace_id"),
                notification_id=payload.get("notification_id"),
                incident_id=payload.get("incident_id"),
                error=payload.get("error"),
                args=list(payload.get("args") or []),
                kwargs=dict(payload.get("kwargs") or {}),
                failed_at=payload.get("failed_at"),
            )
        )
    return total_items, items


def _validate_ops_limit(limit: int) -> None:
    if limit > OPS_ENDPOINT_MAX_LIMIT:
        raise HTTPException(
            status_code=400,
            detail=f"limit exceeds maximum of {OPS_ENDPOINT_MAX_LIMIT}",
        )


def _calculate_sla_metrics(db: Session, hours: int = 24) -> schemas.SLAMetricsResponse:
    window_end = datetime.now(timezone.utc)
    window_start = window_end - timedelta(hours=hours)

    total_incidents = (
        db.query(func.count(models.Incident.id))
        .filter(models.Incident.created_at >= window_start)
        .scalar()
        or 0
    )

    acknowledged_incidents = (
        db.query(func.count(models.Incident.id))
        .filter(
            models.Incident.created_at >= window_start,
            models.Incident.acknowledged_at.isnot(None),
        )
        .scalar()
        or 0
    )

    average_tta_seconds = (
        db.query(
            func.avg(
                func.extract(
                    "epoch",
                    models.Incident.acknowledged_at - models.Incident.created_at,
                )
            )
        )
        .filter(
            models.Incident.created_at >= window_start,
            models.Incident.acknowledged_at.isnot(None),
        )
        .scalar()
    )

    incidents_by_status_rows = (
        db.query(
            models.Incident.status,
            func.count(models.Incident.id),
        )
        .filter(models.Incident.created_at >= window_start)
        .group_by(models.Incident.status)
        .all()
    )

    incidents_by_status = [
        schemas.IncidentStatusCount(status=row[0], count=int(row[1] or 0))
        for row in incidents_by_status_rows
    ]

    acknowledgement_rate = (
        float(acknowledged_incidents) / float(total_incidents)
        if total_incidents
        else 0.0
    )

    return schemas.SLAMetricsResponse(
        hours=hours,
        window_start=window_start,
        window_end=window_end,
        total_incidents=total_incidents,
        acknowledged_incidents=acknowledged_incidents,
        acknowledgement_rate=acknowledgement_rate,
        average_tta_seconds=(
            float(average_tta_seconds) if average_tta_seconds is not None else None
        ),
        incidents_by_status=incidents_by_status,
    )


def _build_operational_alerts(
    db: Session,
    queue_metrics: schemas.QueueMetricsResponse,
    metrics_snapshot: dict[str, object],
) -> list[schemas.OperationalAlert]:
    alerts: list[schemas.OperationalAlert] = []

    dlq_size = int(metrics_snapshot.get("dlq_size") or queue_metrics.dlq or 0)
    if dlq_size >= ALERT_DLQ_CRIT:
        alerts.append(
            schemas.OperationalAlert(
                alert_type="dlq_size",
                severity="critical",
                message=f"DLQ size is {dlq_size}, at or above critical threshold {ALERT_DLQ_CRIT}.",
                actual=dlq_size,
                threshold=float(ALERT_DLQ_CRIT),
                metadata={"queue": "dlq"},
            )
        )
    elif dlq_size >= ALERT_DLQ_WARN:
        alerts.append(
            schemas.OperationalAlert(
                alert_type="dlq_size",
                severity="warn",
                message=f"DLQ size is {dlq_size}, above warning threshold {ALERT_DLQ_WARN}.",
                actual=dlq_size,
                threshold=float(ALERT_DLQ_WARN),
                metadata={"queue": "dlq"},
            )
        )

    for queue_name_label, queue_length in {
        "dispatch": queue_metrics.dispatch,
        "voice": queue_metrics.voice,
    }.items():
        if queue_length >= ALERT_QUEUE_WARN * 2:
            severity = "critical"
        elif queue_length >= ALERT_QUEUE_WARN:
            severity = "warn"
        else:
            continue
        alerts.append(
            schemas.OperationalAlert(
                alert_type=f"queue_backlog:{queue_name_label}",
                severity=severity,
                message=(
                    f"{queue_name_label} queue length is {queue_length}, "
                    f"at or above threshold {ALERT_QUEUE_WARN}."
                ),
                actual=queue_length,
                threshold=float(ALERT_QUEUE_WARN),
                metadata={"queue": queue_name_label},
            )
        )

    sla_metrics = _calculate_sla_metrics(db, hours=24)
    ack_rate = sla_metrics.acknowledgement_rate
    if sla_metrics.total_incidents > 0 and ack_rate < ALERT_ACK_RATE_WARN:
        severity = "critical" if ack_rate < (ALERT_ACK_RATE_WARN * 0.5) else "warn"
        alerts.append(
            schemas.OperationalAlert(
                alert_type="ack_rate_24h",
                severity=severity,
                message=(
                    f"24h acknowledgement rate is {ack_rate:.3f}, below warning threshold "
                    f"{ALERT_ACK_RATE_WARN:.3f}."
                ),
                actual=ack_rate,
                threshold=float(ALERT_ACK_RATE_WARN),
                metadata={
                    "hours": 24,
                    "total_incidents": sla_metrics.total_incidents,
                    "acknowledged_incidents": sla_metrics.acknowledged_incidents,
                },
            )
        )

    return alerts


def _verify_voice_webhook_signature(
    body: bytes,
    timestamp_header: str | None,
    signature_header: str | None,
) -> tuple[bool, str]:
    if not VOICE_WEBHOOK_SECRET:
        return True, "skipped"
    if not timestamp_header:
        return False, "missing_timestamp"
    if not signature_header:
        return False, "missing_signature"

    try:
        timestamp = int(timestamp_header)
    except ValueError:
        return False, "invalid_timestamp"

    now_ts = int(time.time())
    if abs(now_ts - timestamp) > VOICE_WEBHOOK_MAX_SKEW_SECONDS:
        return False, "timestamp_expired"

    canonical = f"{timestamp_header}.".encode("utf-8") + body
    expected_signature = hmac.new(
        VOICE_WEBHOOK_SECRET.encode("utf-8"),
        canonical,
        hashlib.sha256,
    ).hexdigest()
    received_signature = signature_header.strip()
    if received_signature.startswith("sha256="):
        received_signature = received_signature.split("=", 1)[1]

    if not hmac.compare_digest(expected_signature, received_signature):
        return False, "invalid_signature"

    return True, "ok"


def _verify_json_webhook_signature(
    body: bytes,
    signature_header: str | None,
    secret: str | None,
    *,
    header_prefix: str | None = None,
) -> tuple[bool, str]:
    if not secret:
        return True, "skipped"
    if not signature_header:
        return False, "missing_signature"

    received_signature = signature_header.strip()
    if header_prefix and received_signature.startswith(f"{header_prefix}="):
        received_signature = received_signature.split("=", 1)[1]

    expected_signature = hmac.new(
        secret.encode("utf-8"),
        body,
        hashlib.sha256,
    ).hexdigest()
    if not hmac.compare_digest(expected_signature, received_signature):
        return False, "invalid_signature"
    return True, "ok"


def _coerce_payload_dict(payload: object) -> dict[str, object]:
    if isinstance(payload, dict):
        return dict(payload)
    return {}


def _string_or_empty(value: object) -> str:
    if value is None:
        return ""
    if isinstance(value, (dict, list)):
        return json.dumps(value, sort_keys=True, ensure_ascii=False)
    return str(value)


def _normalize_severity(level: object, default: str = "WARN") -> str:
    normalized = str(level or "").strip().lower()
    if normalized in {"fatal", "panic", "emergency", "alert"}:
        return "FATAL"
    if normalized in {"critical", "crit", "regression"}:
        return "CRITICAL"
    if normalized in {"error", "err", "failed", "failure"}:
        return "ERROR"
    if normalized in {"warn", "warning", "low"}:
        return "WARN"
    if normalized in {"info", "notice", "debug", "trace"}:
        return "INFO"
    return default


def _build_sentry_event(payload: dict[str, object]) -> schemas.EventIncoming:
    event_name = _string_or_empty(
        payload.get("action") or payload.get("event") or "issue"
    )
    level = payload.get("level") or payload.get("severity") or event_name
    project = _string_or_empty(
        payload.get("project") or payload.get("project_name") or "sentry"
    )
    culprit = _string_or_empty(
        payload.get("culprit")
        or payload.get("title")
        or payload.get("message")
        or "Sentry event"
    )
    issue_id = _string_or_empty(
        payload.get("issue_id") or payload.get("id") or payload.get("event_id")
    )
    normalized_issue_id = (
        issue_id
        or hashlib.sha256(
            json.dumps(payload, sort_keys=True, default=str).encode("utf-8")
        ).hexdigest()[:16]
    )
    issue_url = _string_or_empty(payload.get("url") or payload.get("web_url"))
    title = f"Sentry {event_name}: {project}"
    message_parts = [culprit]
    if issue_url:
        message_parts.append(issue_url)
    return schemas.EventIncoming(
        external_event_id=f"sentry:{normalized_issue_id}:{event_name.lower().replace(' ', '-')}",
        source="sentry",
        severity=_normalize_severity(level, default="ERROR"),
        service=project,
        title=title[:255],
        message="\n".join(part for part in message_parts if part)[:5000],
        payload_json=payload,
        schedule_at=None,
        dedupe_key=hashlib.sha256(
            f"sentry|{project}|{event_name}|{culprit}".encode("utf-8")
        ).hexdigest(),
    )


def _build_langfuse_event(payload: dict[str, object]) -> schemas.EventIncoming:
    event_name = _string_or_empty(
        payload.get("event") or payload.get("type") or "event"
    )
    project = _string_or_empty(
        payload.get("project") or payload.get("project_id") or "langfuse"
    )
    trace_id = _string_or_empty(
        payload.get("trace_id") or payload.get("id") or payload.get("object_id")
    )
    if not trace_id:
        trace_id = hashlib.sha256(
            json.dumps(payload, sort_keys=True, default=str).encode("utf-8")
        ).hexdigest()[:16]

    level = payload.get("level") or payload.get("severity")
    score_value = payload.get("score")
    score_name = _string_or_empty(payload.get("score_name") or payload.get("name"))
    trace_name = _string_or_empty(
        payload.get("trace_name") or payload.get("observation_name")
    )
    message = _string_or_empty(
        payload.get("message") or payload.get("status_message") or payload.get("error")
    )

    if level is None and isinstance(score_value, (int, float)):
        level = "warn" if float(score_value) < 0.5 else "info"
    if level is None and event_name.lower() in {"trace.error", "trace_failed", "error"}:
        level = "error"

    title_bits = ["Langfuse", event_name]
    if score_name:
        title_bits.append(score_name)
    elif trace_name:
        title_bits.append(trace_name)
    title = " - ".join(bit for bit in title_bits if bit)

    summary_parts = [part for part in [message, f"trace_id={trace_id}"] if part]
    if isinstance(score_value, (int, float)):
        summary_parts.append(f"score={float(score_value):.3f}")

    dedupe_basis = (
        f"langfuse|{project}|{event_name}|{trace_name}|{score_name}|{message}"
    )
    return schemas.EventIncoming(
        external_event_id=f"langfuse:{trace_id}:{event_name.lower().replace(' ', '-')}",
        source="langfuse",
        severity=_normalize_severity(level, default="WARN"),
        service=project,
        title=title[:255],
        message="\n".join(summary_parts)[:5000],
        payload_json=payload,
        schedule_at=None,
        dedupe_key=hashlib.sha256(dedupe_basis.encode("utf-8")).hexdigest(),
    )


def _fetch_oasis_radar_entries(
    query: str,
    lookback_seconds: int,
    limit: int,
) -> list[dict]:
    if not OASIS_RADAR_ENABLED:
        return []

    end_ns = int(time.time() * 1_000_000_000)
    lookback_start_ns = end_ns - max(1, lookback_seconds) * 1_000_000_000
    cursor_raw = redis_conn.get(prefixed_redis_key(OASIS_RADAR_CURSOR_KEY))
    cursor_ns = 0
    if cursor_raw is not None:
        try:
            cursor_ns = int(
                cursor_raw.decode("utf-8")
                if isinstance(cursor_raw, bytes)
                else cursor_raw
            )
        except (TypeError, ValueError):
            cursor_ns = 0
    start_ns = max(lookback_start_ns, cursor_ns + 1)
    encoded_query = urlencode(
        {
            "query": query,
            "start": str(start_ns),
            "end": str(end_ns),
            "limit": str(max(1, limit)),
            "direction": "forward",
        }
    )
    endpoint = (
        f"{OASIS_RADAR_LOKI_URL.rstrip('/')}/loki/api/v1/query_range?{encoded_query}"
    )
    request = UrlRequest(endpoint, method="GET")

    raw = ""
    last_error: Exception | None = None
    for attempt in range(3):
        try:
            with urlopen(request, timeout=15) as response:
                raw = response.read().decode("utf-8")
            last_error = None
            break
        except Exception as exc:
            last_error = exc
            if attempt < 2:
                time.sleep(0.5 * (2**attempt))
    if last_error is not None:
        raise last_error
    parsed = json.loads(raw)
    result = parsed.get("data", {}).get("result", [])

    entries: list[dict] = []
    for stream in result:
        labels = stream.get("stream", {})
        for value in stream.get("values", []):
            if not isinstance(value, list) or len(value) < 2:
                continue
            entries.append(
                {
                    "timestamp_ns": value[0],
                    "line": value[1],
                    "labels": labels,
                }
            )
    return entries


def _oasis_radar_service(labels: dict[str, object]) -> str:
    for key in ("compose_service", "service", "app", "job", "container"):
        value = labels.get(key)
        if value:
            return str(value)
    return "oasis-radar"


def _oasis_radar_severity(line: str, labels: dict[str, object]) -> str:
    for key in ("level", "severity", "log_level"):
        value = labels.get(key)
        if value:
            level = str(value).lower()
            if level in {"fatal", "critical", "crit", "panic", "emergency", "alert"}:
                return "CRITICAL"
            if level in {"error", "err"}:
                return "ERROR"
            if level in {"warning", "warn"}:
                return "WARN"
            if level in {"info", "information"}:
                return "INFO"

    candidate = line
    try:
        if line.startswith("{"):
            parsed = json.loads(line)
            if isinstance(parsed, dict):
                for key in ("level", "severity", "log_level", "status"):
                    value = parsed.get(key)
                    if value:
                        candidate = str(value)
                        break
    except Exception:
        pass

    text = candidate.lower()
    if re.search(r"(fatal|critical|panic|emergency|alert)", text):
        return "CRITICAL"
    if re.search(r"(error|exception|fail(ed|ure)?)", text):
        return "ERROR"
    if re.search(r"(warn|warning)", text):
        return "WARN"
    return "INFO"


def _check_oasis_radar_connectivity() -> tuple[bool, str]:
    if not OASIS_RADAR_ENABLED:
        return False, "disabled"
    endpoint = f"{OASIS_RADAR_LOKI_URL.rstrip('/')}/ready"
    try:
        request = UrlRequest(endpoint, method="GET")
        with urlopen(request, timeout=5) as response:
            if int(response.status) >= 400:
                return False, f"http_{response.status}"
        return True, "ok"
    except HTTPError as exc:
        return False, f"http_{exc.code}"
    except URLError as exc:
        return False, f"network:{exc.reason}"
    except Exception as exc:
        return False, str(exc)


@app.get("/health")
def health_check():
    return {"status": "ok"}


@app.get("/health/deps", response_model=schemas.HealthDepsResponse)
def health_dependencies(db: Session = Depends(get_db)):
    postgres_status = _dependency_status(False)
    redis_status = _dependency_status(False)

    try:
        db.execute(text("SELECT 1"))
        postgres_status = _dependency_status(True)
    except Exception as exc:
        postgres_status = _dependency_status(False, str(exc))

    try:
        redis_conn.ping()
        redis_status = _dependency_status(True)
    except Exception as exc:
        redis_status = _dependency_status(False, str(exc))

    overall_ok = postgres_status.status == "ok" and redis_status.status == "ok"
    return schemas.HealthDepsResponse(
        postgres=postgres_status,
        redis=redis_status,
        overall="ok" if overall_ok else "down",
    )


@app.get("/health/integrations", response_model=schemas.HealthIntegrationsResponse)
def health_integrations():
    integrations: list[schemas.IntegrationStatusDetail] = []

    voice_provider = (VOICE_PROVIDER or "mock").strip().lower()
    if voice_provider == "twilio":
        configured = all([TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN, TWILIO_FROM_NUMBER])
        integrations.append(
            schemas.IntegrationStatusDetail(
                name="voice:twilio",
                status="ok" if configured else "down",
                detail=None if configured else "Missing Twilio credentials",
            )
        )
    elif voice_provider == "signalwire":
        configured = all(
            [
                SIGNALWIRE_SPACE_URL,
                SIGNALWIRE_PROJECT_ID,
                SIGNALWIRE_API_TOKEN,
                SIGNALWIRE_FROM_NUMBER,
            ]
        )
        integrations.append(
            schemas.IntegrationStatusDetail(
                name="voice:signalwire",
                status="ok" if configured else "down",
                detail=None if configured else "Missing SignalWire credentials",
            )
        )
    else:
        integrations.append(
            schemas.IntegrationStatusDetail(
                name="voice",
                status="disabled",
                detail=f"provider={voice_provider}",
            )
        )

    integrations.append(
        schemas.IntegrationStatusDetail(
            name="sentry",
            status="ok" if bool(SENTRY_DSN) else "disabled",
            detail=None if SENTRY_DSN else "SENTRY_DSN not configured",
        )
    )
    integrations.append(
        schemas.IntegrationStatusDetail(
            name="langfuse",
            status=(
                "ok"
                if bool(LANGFUSE_PUBLIC_KEY and LANGFUSE_SECRET_KEY)
                else "disabled"
            ),
            detail=(
                None
                if (LANGFUSE_PUBLIC_KEY and LANGFUSE_SECRET_KEY)
                else "Langfuse keys not configured"
            ),
        )
    )
    integrations.append(
        schemas.IntegrationStatusDetail(
            name="prometheus",
            status="ok" if SERIEMA_PROMETHEUS_ENABLED else "disabled",
            detail=(
                "/metrics exposed"
                if SERIEMA_PROMETHEUS_ENABLED
                else "SERIEMA_PROMETHEUS_ENABLED=false"
            ),
        )
    )
    if OASIS_RADAR_ENABLED:
        oasis_ok, oasis_detail = _check_oasis_radar_connectivity()
        integrations.append(
            schemas.IntegrationStatusDetail(
                name="oasis-radar",
                status="ok" if oasis_ok else "down",
                detail=f"{OASIS_RADAR_LOKI_URL} ({oasis_detail})",
            )
        )
    else:
        integrations.append(
            schemas.IntegrationStatusDetail(
                name="oasis-radar",
                status="disabled",
                detail="OASIS_RADAR_ENABLED=false",
            )
        )

    statuses = {item.status for item in integrations}
    overall: str
    if "down" in statuses:
        overall = "down"
    elif "disabled" in statuses:
        overall = "degraded"
    else:
        overall = "ok"

    return schemas.HealthIntegrationsResponse(
        overall=overall,
        integrations=integrations,
    )


@app.post("/events/incoming", response_model=schemas.EventIngestResponse)
def ingest_event(event: schemas.EventIncoming, db: Session = Depends(get_db)):
    audit_trace_id = str(uuid.uuid4())

    event_dict = event.model_dump()
    active_rules = (
        db.query(models.Rule)
        .filter(models.Rule.active.is_(True))
        .order_by(models.Rule.priority.asc(), models.Rule.rule_name.asc())
        .all()
    )
    matched_rule = evaluate_rules(event_dict, active_rules)
    effective_dedupe_key = event.dedupe_key or _build_rule_dedupe_key(
        event_dict, matched_rule
    )
    if effective_dedupe_key and is_duplicate(f"dedupe:{effective_dedupe_key}"):
        db.add(
            models.AuditLog(
                trace_id=audit_trace_id,
                incident_id=None,
                action=models.AuditAction.DUPLICATED_EVENT,
                details_json={
                    "source": event.source,
                    "external_event_id": event.external_event_id,
                    "dedupe_key": effective_dedupe_key,
                },
            )
        )
        db.commit()
        notify_event(
            "seriema.event.duplicate",
            {
                "trace_id": audit_trace_id,
                "source": event.source,
                "external_event_id": event.external_event_id,
                "dedupe_key": effective_dedupe_key,
            },
            trace_id=audit_trace_id,
        )
        return schemas.EventIngestResponse(
            status="ignored",
            reason="duplicate",
            incident_id=None,
            matched_rule=False,
            trace_id=audit_trace_id,
        )

    incident = models.Incident(
        external_event_id=event.external_event_id,
        source=event.source,
        severity=event.severity,
        service=event.service,
        title=event.title,
        message=event.message,
        payload_json=event.payload_json,
        status=models.IncidentStatus.OPEN,
        matched_rule_id=matched_rule.id if matched_rule else None,
        dedupe_key=effective_dedupe_key,
    )
    db.add(incident)
    try:
        db.commit()
    except IntegrityError:
        db.rollback()
        existing_incident = (
            db.query(models.Incident)
            .filter(
                models.Incident.source == event.source,
                models.Incident.external_event_id == event.external_event_id,
            )
            .first()
        )
        notify_event(
            "seriema.event.duplicate_source_external_event_id",
            {
                "trace_id": audit_trace_id,
                "source": event.source,
                "external_event_id": event.external_event_id,
                "existing_incident_id": (
                    str(existing_incident.id) if existing_incident else None
                ),
            },
            trace_id=audit_trace_id,
        )
        return schemas.EventIngestResponse(
            status="ignored",
            reason="duplicate_source_external_event_id",
            incident_id=existing_incident.id if existing_incident else None,
            matched_rule=False,
            trace_id=audit_trace_id,
        )
    db.refresh(incident)

    audit = models.AuditLog(
        trace_id=audit_trace_id,
        incident_id=incident.id,
        action=models.AuditAction.EVENT_RECEIVED,
        details_json={"payload": event_dict},
    )
    db.add(audit)
    if matched_rule:
        db.add(
            models.AuditLog(
                trace_id=audit_trace_id,
                incident_id=incident.id,
                action=models.AuditAction.RULE_MATCHED,
                details_json={"matched_rule_id": str(matched_rule.id)},
            )
        )
    db.commit()

    if matched_rule:
        dispatch_incident.apply_async(
            args=(str(incident.id), audit_trace_id),
            queue=queue_name("dispatch"),
        )
    notify_event(
        "seriema.event.accepted",
        {
            "trace_id": audit_trace_id,
            "incident_id": str(incident.id),
            "matched_rule": bool(matched_rule),
            "source": incident.source,
            "severity": incident.severity,
        },
        trace_id=audit_trace_id,
    )

    return schemas.EventIngestResponse(
        status="accepted",
        incident_id=incident.id,
        matched_rule=bool(matched_rule),
        trace_id=audit_trace_id,
    )


@app.post("/integrations/sentry/webhook", response_model=schemas.EventIngestResponse)
async def sentry_webhook(request: Request, db: Session = Depends(get_db)):
    raw_body = await request.body()
    signature_ok, _signature_reason = _verify_json_webhook_signature(
        raw_body,
        request.headers.get("Sentry-Hook-Signature")
        or request.headers.get("X-Sentry-Signature"),
        SENTRY_WEBHOOK_SIGNING_SECRET,
        header_prefix="sha256",
    )
    if not signature_ok:
        raise HTTPException(status_code=401, detail="Invalid Sentry webhook signature")

    payload = _coerce_payload_dict(await request.json())
    event = _build_sentry_event(payload)
    return ingest_event(event=event, db=db)


@app.post("/integrations/langfuse/webhook", response_model=schemas.EventIngestResponse)
async def langfuse_webhook(request: Request, db: Session = Depends(get_db)):
    raw_body = await request.body()
    signature_ok, _signature_reason = _verify_json_webhook_signature(
        raw_body,
        request.headers.get("X-Langfuse-Signature")
        or request.headers.get("Langfuse-Signature")
        or request.headers.get("X-Webhook-Signature"),
        LANGFUSE_WEBHOOK_SECRET,
        header_prefix="sha256",
    )
    if not signature_ok:
        raise HTTPException(
            status_code=401, detail="Invalid Langfuse webhook signature"
        )

    payload = _coerce_payload_dict(await request.json())
    event = _build_langfuse_event(payload)
    return ingest_event(event=event, db=db)


@app.post("/integrations/oasis-radar/pull")
def pull_oasis_radar(
    lookback_seconds: int = Query(default=OASIS_RADAR_LOOKBACK_SECONDS, ge=10, le=3600),
    limit: int = Query(default=OASIS_RADAR_PULL_LIMIT, ge=1, le=500),
    x_admin_token: str | None = Header(default=None, alias="X-Admin-Token"),
    db: Session = Depends(get_db),
):
    _require_admin_token(x_admin_token)
    _validate_ops_limit(limit)

    metrics_key = prefixed_redis_key(METRICS_KEY)
    try:
        entries = _fetch_oasis_radar_entries(
            query=OASIS_RADAR_LOGQL_QUERY,
            lookback_seconds=lookback_seconds,
            limit=limit,
        )
    except HTTPError as exc:
        redis_conn.hincrby(metrics_key, "oasis_radar_pull_failed_total", 1)
        redis_conn.hset(metrics_key, "oasis_radar_pull_last_status", "http_error")
        notify_exception(
            exc,
            {
                "stage": "oasis_radar_pull",
                "lookback_seconds": lookback_seconds,
                "limit": limit,
            },
        )
        raise HTTPException(
            status_code=502, detail=f"Oasis Radar HTTP error: {exc.code}"
        ) from exc
    except URLError as exc:
        redis_conn.hincrby(metrics_key, "oasis_radar_pull_failed_total", 1)
        redis_conn.hset(
            metrics_key, "oasis_radar_pull_last_status", "connectivity_error"
        )
        notify_exception(
            exc,
            {
                "stage": "oasis_radar_pull",
                "lookback_seconds": lookback_seconds,
                "limit": limit,
            },
        )
        raise HTTPException(
            status_code=502, detail=f"Oasis Radar connectivity error: {exc.reason}"
        ) from exc
    except Exception as exc:
        redis_conn.hincrby(metrics_key, "oasis_radar_pull_failed_total", 1)
        redis_conn.hset(metrics_key, "oasis_radar_pull_last_status", "unexpected_error")
        notify_exception(
            exc,
            {
                "stage": "oasis_radar_pull",
                "lookback_seconds": lookback_seconds,
                "limit": limit,
            },
        )
        raise HTTPException(
            status_code=500, detail="Unexpected Oasis Radar pull error"
        ) from exc

    if not entries:
        redis_conn.hincrby(metrics_key, "oasis_radar_pull_success_total", 1)
        redis_conn.hset(metrics_key, "oasis_radar_pull_last_status", "ok")
        return {"status": "ok", "fetched": 0, "accepted": 0, "duplicates": 0}

    accepted = 0
    duplicates = 0
    max_timestamp_ns = 0
    for entry in entries:
        labels = entry.get("labels", {})
        line = str(entry.get("line", "")).strip()
        timestamp_ns = str(entry.get("timestamp_ns", ""))
        try:
            max_timestamp_ns = max(max_timestamp_ns, int(timestamp_ns))
        except (TypeError, ValueError):
            pass
        service = _oasis_radar_service(labels if isinstance(labels, dict) else {})
        dedupe_key = hashlib.sha256(
            f"{timestamp_ns}:{service}:{line}".encode("utf-8")
        ).hexdigest()

        if is_duplicate(f"oasis-radar:{dedupe_key}"):
            duplicates += 1
            continue

        severity, title = classify_log_line(
            service,
            line,
            labels if isinstance(labels, dict) else {},
        )
        event = schemas.EventIncoming(
            external_event_id=f"oasis-radar-{timestamp_ns}-{service}",
            source="oasis-radar",
            severity=severity,
            service=service,
            title=title,
            message=line[:5000],
            payload_json={"labels": labels, "timestamp_ns": timestamp_ns},
            schedule_at=None,
            dedupe_key=dedupe_key,
        )
        ingest_event(event=event, db=db)
        accepted += 1

    if max_timestamp_ns > 0:
        redis_conn.set(
            prefixed_redis_key(OASIS_RADAR_CURSOR_KEY), str(max_timestamp_ns)
        )
    redis_conn.hincrby(metrics_key, "oasis_radar_pull_success_total", 1)
    redis_conn.hincrby(metrics_key, "oasis_radar_entries_fetched_total", len(entries))
    redis_conn.hincrby(metrics_key, "oasis_radar_entries_ingested_total", accepted)
    redis_conn.hset(metrics_key, "oasis_radar_pull_last_status", "ok")
    redis_conn.hset(metrics_key, "oasis_radar_pull_last_fetched", len(entries))
    redis_conn.hset(metrics_key, "oasis_radar_pull_last_accepted", accepted)
    redis_conn.hset(metrics_key, "oasis_radar_pull_last_duplicates", duplicates)

    notify_event(
        "seriema.oasis_radar.pull",
        {
            "fetched": len(entries),
            "accepted": accepted,
            "duplicates": duplicates,
            "lookback_seconds": lookback_seconds,
        },
    )
    return {
        "status": "ok",
        "fetched": len(entries),
        "accepted": accepted,
        "duplicates": duplicates,
    }


@app.post("/rules", response_model=schemas.RuleResponse)
def create_rule(rule: schemas.RuleCreate, db: Session = Depends(get_db)):
    db_rule = models.Rule(**rule.model_dump())
    if db_rule.id is None:
        db_rule.id = uuid.uuid4()
    db.add(db_rule)
    db.commit()
    db.refresh(db_rule)
    return _serialize_rule(db_rule)


@app.get("/rules", response_model=schemas.RuleListResponse)
def list_rules(
    active: bool | None = Query(default=None),
    recipient_group_id: uuid.UUID | None = Query(default=None),
    limit: int = Query(20, ge=1),
    offset: int = Query(0, ge=0),
    db: Session = Depends(get_db),
):
    _validate_ops_limit(limit)

    query = db.query(models.Rule)
    if active is not None:
        query = query.filter(models.Rule.active == active)
    if recipient_group_id is not None:
        query = query.filter(models.Rule.recipient_group_id == recipient_group_id)

    rules = query.order_by(
        models.Rule.priority.asc(), models.Rule.rule_name.asc(), models.Rule.id.asc()
    ).all()
    total = len(rules)
    items = rules[offset : offset + limit]

    return schemas.RuleListResponse(
        total=total,
        limit=limit,
        offset=offset,
        items=[_serialize_rule(rule) for rule in items],
    )


@app.patch("/rules/{rule_id}", response_model=schemas.RuleResponse)
def update_rule(
    rule_id: str,
    rule_update: schemas.RuleUpdate,
    db: Session = Depends(get_db),
):
    try:
        rule_uuid = uuid.UUID(rule_id)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail="Invalid rule id") from exc

    db_rule = db.query(models.Rule).filter(models.Rule.id == rule_uuid).first()
    if not db_rule:
        raise HTTPException(status_code=404, detail="Rule not found")

    update_data = {
        "rule_name": rule_update.rule_name,
        "condition_json": rule_update.condition_json,
        "recipient_group_id": rule_update.recipient_group_id,
        "channels": rule_update.channels,
        "active": rule_update.active,
        "priority": rule_update.priority,
        "requires_ack": rule_update.requires_ack,
        "ack_deadline": rule_update.ack_deadline,
        "fallback_policy_json": rule_update.fallback_policy_json,
        "dedupe_window_seconds": rule_update.dedupe_window_seconds,
        "dedupe_fields_json": rule_update.dedupe_fields_json,
        "notification_templates_json": rule_update.notification_templates_json,
        "runbook_url": rule_update.runbook_url,
        "channel_retry_policy_json": rule_update.channel_retry_policy_json,
    }
    provided = {key: value for key, value in update_data.items() if value is not None}
    if not provided:
        raise HTTPException(status_code=400, detail="No rule fields provided")

    if rule_update.recipient_group_id is not None:
        group = (
            db.query(models.Group)
            .filter(models.Group.id == rule_update.recipient_group_id)
            .first()
        )
        if not group:
            raise HTTPException(status_code=404, detail="Group not found")

    for key, value in provided.items():
        setattr(db_rule, key, value)

    db.commit()
    db.refresh(db_rule)
    return _serialize_rule(db_rule)


@app.post("/rules/{rule_id}/toggle", response_model=schemas.RuleResponse)
def toggle_rule(rule_id: str, db: Session = Depends(get_db)):
    try:
        rule_uuid = uuid.UUID(rule_id)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail="Invalid rule id") from exc

    db_rule = db.query(models.Rule).filter(models.Rule.id == rule_uuid).first()
    if not db_rule:
        raise HTTPException(status_code=404, detail="Rule not found")

    db_rule.active = not bool(db_rule.active)
    db.commit()
    db.refresh(db_rule)
    return _serialize_rule(db_rule)


@app.post("/rules/{rule_id}/simulate", response_model=schemas.RuleSimulationResponse)
def simulate_rule(
    rule_id: str,
    payload: dict[str, object],
    db: Session = Depends(get_db),
):
    try:
        rule_uuid = uuid.UUID(rule_id)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail="Invalid rule id") from exc

    db_rule = db.query(models.Rule).filter(models.Rule.id == rule_uuid).first()
    if not db_rule:
        raise HTTPException(status_code=404, detail="Rule not found")

    return _simulate_rule(db_rule, payload)


def _load_contact_or_404(
    contact_id: str, db: Session
) -> tuple[uuid.UUID, models.Contact]:
    try:
        contact_uuid = uuid.UUID(contact_id)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail="Invalid contact id") from exc

    contact = db.query(models.Contact).filter(models.Contact.id == contact_uuid).first()
    if not contact:
        raise HTTPException(status_code=404, detail="Contact not found")
    return contact_uuid, contact


def _load_group_or_404(group_id: str, db: Session) -> tuple[uuid.UUID, models.Group]:
    try:
        group_uuid = uuid.UUID(group_id)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail="Invalid group id") from exc

    group = db.query(models.Group).filter(models.Group.id == group_uuid).first()
    if not group:
        raise HTTPException(status_code=404, detail="Group not found")
    return group_uuid, group


def _has_any_rows(query) -> bool:
    try:
        return query.first() is not None
    except Exception:
        return False


def _serialize_contact(contact: models.Contact) -> schemas.ContactResponse:
    return schemas.ContactResponse(
        id=contact.id,
        name=contact.name,
        email=contact.email,
        phone=contact.phone,
        whatsapp=contact.whatsapp,
        telegram_id=contact.telegram_id,
    )


def _serialize_group(group: models.Group) -> schemas.GroupResponse:
    return schemas.GroupResponse(
        id=group.id,
        name=group.name,
        description=group.description,
    )


def _delete_contact_guard(db: Session, contact_uuid: uuid.UUID) -> None:
    if _has_any_rows(
        db.query(models.Notification).filter(
            models.Notification.contact_id == contact_uuid
        )
    ):
        raise HTTPException(status_code=409, detail="Contact has related notifications")
    if _has_any_rows(
        db.query(models.GroupMember).filter(
            models.GroupMember.contact_id == contact_uuid
        )
    ):
        raise HTTPException(status_code=409, detail="Contact has related group members")


def _delete_group_guard(db: Session, group_uuid: uuid.UUID) -> None:
    if _has_any_rows(
        db.query(models.Rule).filter(models.Rule.recipient_group_id == group_uuid)
    ):
        raise HTTPException(status_code=409, detail="Group has related rules")
    if _has_any_rows(
        db.query(models.GroupMember).filter(models.GroupMember.group_id == group_uuid)
    ):
        raise HTTPException(status_code=409, detail="Group has related members")


@app.post("/groups", response_model=schemas.GroupResponse)
def create_group(group: schemas.GroupCreate, db: Session = Depends(get_db)):
    db_group = models.Group(**group.model_dump())
    if db_group.id is None:
        db_group.id = uuid.uuid4()
    db.add(db_group)
    db.commit()
    db.refresh(db_group)
    return _serialize_group(db_group)


@app.post("/contacts", response_model=schemas.ContactResponse)
def create_contact(contact: schemas.ContactCreate, db: Session = Depends(get_db)):
    db_contact = models.Contact(**contact.model_dump())
    if db_contact.id is None:
        db_contact.id = uuid.uuid4()
    db.add(db_contact)
    db.commit()
    db.refresh(db_contact)
    return _serialize_contact(db_contact)


@app.get("/contacts", response_model=schemas.ContactListResponse)
def list_contacts(
    limit: int = Query(20, ge=1),
    offset: int = Query(0, ge=0),
    db: Session = Depends(get_db),
):
    _validate_ops_limit(limit)
    contacts = (
        db.query(models.Contact)
        .order_by(models.Contact.name.asc(), models.Contact.id.asc())
        .all()
    )
    return schemas.ContactListResponse(
        total=len(contacts),
        limit=limit,
        offset=offset,
        items=[
            _serialize_contact(contact) for contact in contacts[offset : offset + limit]
        ],
    )


@app.get("/contacts/{contact_id}", response_model=schemas.ContactResponse)
def get_contact(contact_id: str, db: Session = Depends(get_db)):
    _, contact = _load_contact_or_404(contact_id, db)
    return _serialize_contact(contact)


@app.patch("/contacts/{contact_id}", response_model=schemas.ContactResponse)
def update_contact(
    contact_id: str,
    contact_update: schemas.ContactUpdate,
    db: Session = Depends(get_db),
):
    _, contact = _load_contact_or_404(contact_id, db)
    update_data = {
        key: value
        for key, value in contact_update.model_dump().items()
        if value is not None
    }
    if not update_data:
        raise HTTPException(status_code=400, detail="No contact fields provided")
    for key, value in update_data.items():
        setattr(contact, key, value)
    db.commit()
    db.refresh(contact)
    return _serialize_contact(contact)


@app.delete("/contacts/{contact_id}", status_code=204)
def delete_contact(contact_id: str, db: Session = Depends(get_db)):
    contact_uuid, contact = _load_contact_or_404(contact_id, db)
    _delete_contact_guard(db, contact_uuid)
    db.delete(contact)
    db.commit()
    return Response(status_code=204)


@app.get("/groups", response_model=schemas.GroupListResponse)
def list_groups(
    limit: int = Query(20, ge=1),
    offset: int = Query(0, ge=0),
    db: Session = Depends(get_db),
):
    _validate_ops_limit(limit)
    groups = (
        db.query(models.Group)
        .order_by(models.Group.name.asc(), models.Group.id.asc())
        .all()
    )
    return schemas.GroupListResponse(
        total=len(groups),
        limit=limit,
        offset=offset,
        items=[_serialize_group(group) for group in groups[offset : offset + limit]],
    )


@app.get("/groups/{group_id}", response_model=schemas.GroupResponse)
def get_group(group_id: str, db: Session = Depends(get_db)):
    _, group = _load_group_or_404(group_id, db)
    return _serialize_group(group)


@app.patch("/groups/{group_id}", response_model=schemas.GroupResponse)
def update_group(
    group_id: str,
    group_update: schemas.GroupUpdate,
    db: Session = Depends(get_db),
):
    _, group = _load_group_or_404(group_id, db)
    update_data = {
        key: value
        for key, value in group_update.model_dump().items()
        if value is not None
    }
    if not update_data:
        raise HTTPException(status_code=400, detail="No group fields provided")
    for key, value in update_data.items():
        setattr(group, key, value)
    db.commit()
    db.refresh(group)
    return _serialize_group(group)


@app.delete("/groups/{group_id}", status_code=204)
def delete_group(group_id: str, db: Session = Depends(get_db)):
    group_uuid, group = _load_group_or_404(group_id, db)
    _delete_group_guard(db, group_uuid)
    db.delete(group)
    db.commit()
    return Response(status_code=204)


@app.post("/groups/{group_id}/members", response_model=schemas.GroupMemberResponse)
def add_group_member(
    group_id: str, member: schemas.GroupMemberCreate, db: Session = Depends(get_db)
):
    try:
        group_uuid = uuid.UUID(group_id)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail="Invalid group id") from exc

    group = db.query(models.Group).filter(models.Group.id == group_uuid).first()
    if not group:
        raise HTTPException(status_code=404, detail="Group not found")

    contact = (
        db.query(models.Contact).filter(models.Contact.id == member.contact_id).first()
    )
    if not contact:
        raise HTTPException(status_code=404, detail="Contact not found")

    existing = (
        db.query(models.GroupMember)
        .filter(
            models.GroupMember.group_id == group_uuid,
            models.GroupMember.contact_id == member.contact_id,
        )
        .first()
    )
    if not existing:
        db_member = models.GroupMember(
            group_id=group_uuid, contact_id=member.contact_id
        )
        db.add(db_member)
        db.commit()

    return schemas.GroupMemberResponse(
        group_id=group_uuid, contact_id=member.contact_id
    )


@app.delete("/groups/{group_id}/members/{contact_id}", status_code=204)
def delete_group_member(group_id: str, contact_id: str, db: Session = Depends(get_db)):
    group_uuid, _ = _load_group_or_404(group_id, db)
    try:
        contact_uuid = uuid.UUID(contact_id)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail="Invalid contact id") from exc

    contact = db.query(models.Contact).filter(models.Contact.id == contact_uuid).first()
    if not contact:
        raise HTTPException(status_code=404, detail="Contact not found")

    membership = (
        db.query(models.GroupMember)
        .filter(
            models.GroupMember.group_id == group_uuid,
            models.GroupMember.contact_id == contact_uuid,
        )
        .first()
    )
    if not membership:
        raise HTTPException(status_code=404, detail="Membership not found")

    db.delete(membership)
    db.commit()
    return Response(status_code=204)


@app.get("/groups/{group_id}/members", response_model=list[schemas.GroupMemberResponse])
def list_group_members(group_id: str, db: Session = Depends(get_db)):
    try:
        group_uuid = uuid.UUID(group_id)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail="Invalid group id") from exc

    group = db.query(models.Group).filter(models.Group.id == group_uuid).first()
    if not group:
        raise HTTPException(status_code=404, detail="Group not found")

    members = (
        db.query(models.GroupMember)
        .filter(models.GroupMember.group_id == group_uuid)
        .all()
    )
    return [
        schemas.GroupMemberResponse(
            group_id=member.group_id, contact_id=member.contact_id
        )
        for member in members
    ]


@app.get("/incidents/{incident_id}", response_model=schemas.IncidentDetailsResponse)
def get_incident(incident_id: str, db: Session = Depends(get_db)):
    try:
        incident_uuid = uuid.UUID(incident_id)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail="Invalid incident id") from exc

    incident = (
        db.query(models.Incident).filter(models.Incident.id == incident_uuid).first()
    )
    if not incident:
        raise HTTPException(status_code=404, detail="Incident not found")

    audit_logs = (
        db.query(models.AuditLog)
        .filter(models.AuditLog.incident_id == incident_uuid)
        .order_by(models.AuditLog.created_at)
        .all()
    )

    return schemas.IncidentDetailsResponse(
        incident=_serialize_incident(incident),
        logs=[_serialize_audit(log) for log in audit_logs],
    )


@app.get(
    "/incidents/{incident_id}/timeline", response_model=schemas.IncidentTimelineResponse
)
def get_incident_timeline(
    incident_id: str,
    action: models.AuditAction | None = Query(default=None),
    limit: int = Query(20, ge=1),
    offset: int = Query(0, ge=0),
    db: Session = Depends(get_db),
):
    try:
        incident_uuid = uuid.UUID(incident_id)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail="Invalid incident id") from exc

    incident = (
        db.query(models.Incident).filter(models.Incident.id == incident_uuid).first()
    )
    if not incident:
        raise HTTPException(status_code=404, detail="Incident not found")

    _validate_ops_limit(limit)

    query = db.query(models.AuditLog).filter(
        models.AuditLog.incident_id == incident_uuid
    )
    if action is not None:
        query = query.filter(models.AuditLog.action == action)

    total = query.count()
    timeline_items = (
        query.order_by(models.AuditLog.created_at.asc(), models.AuditLog.id.asc())
        .offset(offset)
        .limit(limit)
        .all()
    )

    return schemas.IncidentTimelineResponse(
        total=total,
        limit=limit,
        offset=offset,
        items=[_serialize_audit(log) for log in timeline_items],
    )


@app.get("/incidents/{incident_id}/export")
def export_incident_timeline(incident_id: str, db: Session = Depends(get_db)):
    try:
        incident_uuid = uuid.UUID(incident_id)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail="Invalid incident id") from exc

    incident = (
        db.query(models.Incident).filter(models.Incident.id == incident_uuid).first()
    )
    if not incident:
        raise HTTPException(status_code=404, detail="Incident not found")

    audit_logs = (
        db.query(models.AuditLog)
        .filter(models.AuditLog.incident_id == incident_uuid)
        .order_by(models.AuditLog.created_at.asc())
        .all()
    )

    markdown = f"# Incident Report: {incident.title}\n\n"
    markdown += f"**ID**: `{incident.id}`\n"
    markdown += f"**Source**: `{incident.source}`\n"
    markdown += f"**Service**: `{incident.service or 'N/A'}`\n"
    markdown += f"**Severity**: `{incident.severity}`\n"
    markdown += f"**Status**: `{incident.status}`\n"
    markdown += f"**Created At**: `{incident.created_at}`\n"
    if incident.acknowledged_at:
        markdown += f"**Acknowledged At**: `{incident.acknowledged_at}` by {incident.acknowledged_by}\n"

    markdown += (
        f"\n## Description\n{incident.message or 'No description provided.'}\n\n"
    )
    markdown += "## Timeline\n\n"
    for log in audit_logs:
        markdown += f"- **{log.created_at}** [{log.action.name}] "
        if log.details_json:
            markdown += f"`{json.dumps(log.details_json)}`"
        markdown += "\n"

    return Response(
        content=markdown,
        media_type="text/markdown",
        headers={
            "Content-Disposition": f"attachment; filename=incident-{incident.id}.md"
        },
    )


@app.post(
    "/incidents/{incident_id}/ack", response_model=schemas.IncidentLifecycleResponse
)
def acknowledge_incident(
    incident_id: str,
    payload: schemas.AckIncidentRequest,
    db: Session = Depends(get_db),
):
    try:
        incident_uuid = uuid.UUID(incident_id)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail="Invalid incident id") from exc

    incident = (
        db.query(models.Incident).filter(models.Incident.id == incident_uuid).first()
    )
    if not incident:
        raise HTTPException(status_code=404, detail="Incident not found")

    if incident.status != models.IncidentStatus.OPEN:
        raise HTTPException(status_code=409, detail="Incident is not open")

    acknowledged_at = datetime.now(timezone.utc)
    incident.status = models.IncidentStatus.ACKNOWLEDGED
    incident.acknowledged_at = acknowledged_at
    if payload.acknowledged_by is not None:
        incident.acknowledged_by = payload.acknowledged_by
    incident.updated_at = acknowledged_at

    trace_id = _get_or_create_trace_id(db, incident.id)
    db.add(
        models.AuditLog(
            trace_id=trace_id,
            incident_id=incident.id,
            action=models.AuditAction.ACK_RECEIVED,
            details_json={
                "acknowledged_by": incident.acknowledged_by,
                "source": "api",
                "incident_status": "ACKNOWLEDGED",
            },
        )
    )
    db.commit()
    notify_event(
        "seriema.incident.acknowledged",
        {
            "incident_id": str(incident.id),
            "acknowledged_by": incident.acknowledged_by,
            "source": "api",
        },
        trace_id=trace_id,
    )

    return schemas.IncidentLifecycleResponse(
        action="ack",
        incident=_serialize_incident(incident),
    )


@app.post(
    "/incidents/{incident_id}/resolve", response_model=schemas.IncidentLifecycleResponse
)
def resolve_incident(
    incident_id: str,
    payload: schemas.ResolveIncidentRequest,
    db: Session = Depends(get_db),
):
    try:
        incident_uuid = uuid.UUID(incident_id)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail="Invalid incident id") from exc

    incident = (
        db.query(models.Incident).filter(models.Incident.id == incident_uuid).first()
    )
    if not incident:
        raise HTTPException(status_code=404, detail="Incident not found")

    if incident.status not in {
        models.IncidentStatus.ACKNOWLEDGED,
        models.IncidentStatus.ESCALATED,
    }:
        raise HTTPException(
            status_code=409, detail="Incident cannot be resolved from current status"
        )

    resolved_at = datetime.now(timezone.utc)
    incident.status = models.IncidentStatus.RESOLVED
    incident.updated_at = resolved_at

    trace_id = _get_or_create_trace_id(db, incident.id)
    db.add(
        models.AuditLog(
            trace_id=trace_id,
            incident_id=incident.id,
            action=models.AuditAction.CALLBACK_RECEIVED,
            details_json={
                "resolved_by": payload.resolved_by,
                "note": payload.note,
                "source": "api",
                "incident_status": "RESOLVED",
            },
        )
    )
    db.commit()
    notify_event(
        "seriema.incident.resolved",
        {
            "incident_id": str(incident.id),
            "resolved_by": payload.resolved_by,
            "source": "api",
        },
        trace_id=trace_id,
    )

    return schemas.IncidentLifecycleResponse(
        action="resolve",
        incident=_serialize_incident(incident),
    )


@app.get("/incidents", response_model=schemas.IncidentListResponse)
def list_incidents(
    status: models.IncidentStatus | None = Query(default=None),
    source: str | None = Query(default=None),
    severity: str | None = Query(default=None),
    limit: int = Query(20, ge=1, le=OPS_ENDPOINT_MAX_LIMIT),
    offset: int = Query(0, ge=0),
    db: Session = Depends(get_db),
):
    query = db.query(models.Incident)
    count_query = db.query(func.count(models.Incident.id))

    if status is not None:
        query = query.filter(models.Incident.status == status)
        count_query = count_query.filter(models.Incident.status == status)
    if source:
        query = query.filter(models.Incident.source == source)
        count_query = count_query.filter(models.Incident.source == source)
    if severity:
        query = query.filter(models.Incident.severity == severity)
        count_query = count_query.filter(models.Incident.severity == severity)

    total = int(count_query.scalar() or 0)
    incidents = (
        query.order_by(models.Incident.created_at.desc(), models.Incident.id.desc())
        .offset(offset)
        .limit(limit)
        .all()
    )

    return schemas.IncidentListResponse(
        total=total,
        limit=limit,
        offset=offset,
        items=[_serialize_incident(incident) for incident in incidents],
    )


@app.get("/metrics/sla", response_model=schemas.SLAMetricsResponse)
def get_sla_metrics(
    hours: int = Query(24, ge=1, le=720),
    db: Session = Depends(get_db),
):
    return _calculate_sla_metrics(db, hours=hours)


@app.get("/metrics/queues", response_model=schemas.QueueMetricsResponse)
def get_queue_metrics():
    return schemas.QueueMetricsResponse(
        dispatch=_queue_length(queue_name("dispatch")),
        voice=_queue_length(queue_name("voice")),
        telegram=_queue_length(queue_name("telegram")),
        email=_queue_length(queue_name("email")),
        escalation=_queue_length(queue_name("escalation")),
        dlq=_queue_length(prefixed_redis_key(DLQ_QUEUE_NAME)),
    )


@app.get("/metrics/ops", response_model=schemas.OpsMetricsResponse)
def get_ops_metrics():
    raw_metrics = redis_conn.hgetall(prefixed_redis_key(METRICS_KEY))
    parsed_metrics = {
        str(
            key.decode("utf-8") if isinstance(key, bytes) else key
        ): _metric_value_from_redis(value)
        for key, value in raw_metrics.items()
    }
    return schemas.OpsMetricsResponse(
        redis_key=prefixed_redis_key(METRICS_KEY),
        metrics=parsed_metrics,
    )


@app.get("/alerts/ops", response_model=schemas.OpsAlertsResponse)
def get_ops_alerts(db: Session = Depends(get_db)):
    raw_metrics = redis_conn.hgetall(prefixed_redis_key(METRICS_KEY))
    parsed_metrics = {
        str(
            key.decode("utf-8") if isinstance(key, bytes) else key
        ): _metric_value_from_redis(value)
        for key, value in raw_metrics.items()
    }
    queue_metrics = schemas.QueueMetricsResponse(
        dispatch=_queue_length(queue_name("dispatch")),
        voice=_queue_length(queue_name("voice")),
        telegram=_queue_length(queue_name("telegram")),
        email=_queue_length(queue_name("email")),
        escalation=_queue_length(queue_name("escalation")),
        dlq=_queue_length(prefixed_redis_key(DLQ_QUEUE_NAME)),
    )
    alerts = _build_operational_alerts(db, queue_metrics, parsed_metrics)
    return schemas.OpsAlertsResponse(
        overall_severity=_overall_severity(alerts),
        alert_count=len(alerts),
        alerts=alerts,
        metrics=parsed_metrics,
        queue_metrics=queue_metrics,
    )


@app.get("/ops/dlq/preview", response_model=schemas.DLQPreviewResponse)
def preview_dlq(
    limit: int = Query(20, ge=1),
    x_admin_token: str | None = Header(default=None, alias="X-Admin-Token"),
):
    _require_admin_token(x_admin_token)
    _validate_ops_limit(limit)
    total_items, items = _dlq_items(limit)
    return schemas.DLQPreviewResponse(limit=limit, total_items=total_items, items=items)


@app.post("/ops/dlq/replay", response_model=schemas.DLQReplayResponse)
def replay_dlq_operations(
    limit: int = Query(20, ge=1),
    x_admin_token: str | None = Header(default=None, alias="X-Admin-Token"),
):
    _require_admin_token(x_admin_token)
    _validate_ops_limit(limit)
    result = replay_dlq.apply_async(args=(limit,))
    if celery_app.conf.task_always_eager:
        payload = result.get(propagate=False)
        return schemas.DLQReplayResponse(
            status="completed",
            limit=limit,
            result=payload if isinstance(payload, dict) else {"result": payload},
        )

    return schemas.DLQReplayResponse(
        status="submitted",
        limit=limit,
        task_id=getattr(result, "id", None),
        result=None,
    )


def _coerce_optional_int(value):
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _coerce_optional_bool(value) -> bool:
    if isinstance(value, bool):
        return value
    if value is None or value == "":
        return False
    if isinstance(value, str):
        normalized = value.strip().lower()
        if normalized in {"true", "1", "yes", "y", "on"}:
            return True
        if normalized in {"false", "0", "no", "n", "off"}:
            return False
    try:
        return bool(int(value))
    except (TypeError, ValueError):
        return bool(value)


@app.get("/ops/dlq/replay/last", response_model=schemas.DLQReplayReportResponse)
def get_last_dlq_replay_report():
    raw_report = get_dlq_replay_report() or {}
    normalized = {
        "status": str(raw_report.get("status") or "empty"),
        "started_at": raw_report.get("started_at") or None,
        "finished_at": raw_report.get("finished_at") or None,
        "requested_limit": _coerce_optional_int(raw_report.get("requested_limit")),
        "effective_limit": _coerce_optional_int(raw_report.get("effective_limit")),
        "replayed": _coerce_optional_int(raw_report.get("replayed")) or 0,
        "remaining": _coerce_optional_int(raw_report.get("remaining")) or 0,
        "dry_run": _coerce_optional_bool(raw_report.get("dry_run")),
        "locked": _coerce_optional_bool(raw_report.get("locked")),
    }
    for key, value in raw_report.items():
        if key not in normalized:
            normalized[key] = value
    return schemas.DLQReplayReportResponse(**normalized)


def _evaluate_ops_integration_status(
    db: Session,
) -> schemas.OpsIntegrationStatusResponse:
    evidence: dict[str, object] = {}

    enums_ok = all(
        hasattr(enum_cls, "__members__") and len(enum_cls.__members__) > 0
        for enum_cls in (
            models.IncidentStatus,
            models.NotificationStatus,
            models.AuditAction,
        )
    )
    evidence["enums"] = {
        "incident_statuses": list(models.IncidentStatus.__members__.keys()),
        "notification_statuses": list(models.NotificationStatus.__members__.keys()),
        "audit_actions": list(models.AuditAction.__members__.keys()),
    }

    fallback_contract_ok = (
        "fallback_policy_json" in schemas.RuleCreate.model_fields
        and "fallback_policy_json" in schemas.RuleUpdate.model_fields
    )
    evidence["fallback_contract"] = {
        "rule_create": "fallback_policy_json" in schemas.RuleCreate.model_fields,
        "rule_update": "fallback_policy_json" in schemas.RuleUpdate.model_fields,
    }

    incidents: list[models.Incident] = []
    audit_logs: list[models.AuditLog] = []
    try:
        incidents = db.query(models.Incident).all()
    except Exception as exc:
        evidence["incidents_error"] = str(exc)
    try:
        audit_logs = db.query(models.AuditLog).all()
    except Exception as exc:
        evidence["audit_logs_error"] = str(exc)

    trace_logs = [log for log in audit_logs if getattr(log, "trace_id", "")]
    duplicate_events = [
        log for log in audit_logs if log.action == models.AuditAction.DUPLICATED_EVENT
    ]
    ack_logs = [
        log for log in audit_logs if log.action == models.AuditAction.ACK_RECEIVED
    ]
    acknowledged_incidents = [
        incident
        for incident in incidents
        if incident.status == models.IncidentStatus.ACKNOWLEDGED
    ]
    escalated_logs = [
        log for log in audit_logs if log.action == models.AuditAction.ESCALATED
    ]
    non_open_incidents = [
        incident
        for incident in incidents
        if incident.status != models.IncidentStatus.OPEN
    ]
    non_open_escalated = [
        incident
        for incident in non_open_incidents
        if any(log.incident_id == incident.id for log in escalated_logs)
    ]

    try:
        dlq_report = get_dlq_replay_report() or {}
    except Exception as exc:
        dlq_report = {}
        evidence["dlq_reporting_error"] = str(exc)

    dlq_status = str(dlq_report.get("status") or "").strip()

    trace_propagation_signal = len(trace_logs) > 0
    duplicate_event_signal = len(duplicate_events) > 0
    ack_flow_signal = len(acknowledged_incidents) > 0 or len(ack_logs) > 0
    escalation_guard_signal = len(escalated_logs) > 0 and len(non_open_escalated) == 0
    dlq_reporting_signal = bool(dlq_status)

    evidence.update(
        {
            "trace_propagation": {"trace_logs": len(trace_logs)},
            "duplicate_event": {"duplicate_event_logs": len(duplicate_events)},
            "ack_flow": {
                "acknowledged_incidents": len(acknowledged_incidents),
                "ack_received_logs": len(ack_logs),
            },
            "escalation_guard": {
                "escalated_logs": len(escalated_logs),
                "non_open_incidents": len(non_open_incidents),
                "non_open_escalated_incidents": len(non_open_escalated),
            },
            "dlq_reporting": {
                "report_status": dlq_status or None,
                "has_report": bool(dlq_report),
            },
        }
    )

    return schemas.OpsIntegrationStatusResponse(
        enums_ok=enums_ok,
        fallback_contract_ok=fallback_contract_ok,
        trace_propagation_signal=trace_propagation_signal,
        duplicate_event_signal=duplicate_event_signal,
        ack_flow_signal=ack_flow_signal,
        escalation_guard_signal=escalation_guard_signal,
        dlq_reporting_signal=dlq_reporting_signal,
        evidence=evidence,
    )


@app.get("/ops/integration/status", response_model=schemas.OpsIntegrationStatusResponse)
def get_ops_integration_status(db: Session = Depends(get_db)):
    return _evaluate_ops_integration_status(db)


def _coerce_redis_text(value) -> str | None:
    if value is None:
        return None
    if isinstance(value, bytes):
        value = value.decode("utf-8")
    value = str(value).strip()
    return value or None


def _get_heartbeat_snapshot() -> dict[str, dict[str, str | None]]:
    try:
        raw_metrics = redis_conn.hgetall(prefixed_redis_key(METRICS_KEY))
    except Exception as exc:
        return {"_error": {"error": str(exc)}}

    parsed = {
        str(key.decode("utf-8") if isinstance(key, bytes) else key): _coerce_redis_text(
            value
        )
        for key, value in raw_metrics.items()
    }

    heartbeats: dict[str, dict[str, str | None]] = {}
    for task_name in ("queue_metrics_snapshot", "prune_dlq", "stale_incident_sweeper"):
        heartbeats[task_name] = {
            "last_run_at": parsed.get(f"last_run_at:{task_name}"),
            "last_status": parsed.get(f"last_status:{task_name}"),
        }
    return heartbeats


def _evaluate_ops_readiness(db: Session) -> schemas.OpsReadinessResponse:
    integration = _evaluate_ops_integration_status(db)
    heartbeats = _get_heartbeat_snapshot()

    checks: list[schemas.OpsReadinessCheck] = []
    blockers: list[str] = []
    score = 100

    def add_check(
        name: str, passed: bool, details: str, weight: int, blocker: bool = False
    ) -> None:
        nonlocal score
        checks.append(
            schemas.OpsReadinessCheck(name=name, passed=passed, details=details)
        )
        if not passed:
            score -= weight
            if blocker:
                blockers.append(name)

    add_check(
        "integration_enums",
        integration.enums_ok,
        (
            "Incident, notification and audit enums are available."
            if integration.enums_ok
            else "One or more enum classes are missing members."
        ),
        20,
        blocker=True,
    )
    add_check(
        "fallback_contract",
        integration.fallback_contract_ok,
        (
            "Rule fallback policy contract is available."
            if integration.fallback_contract_ok
            else "Rule fallback policy contract is unavailable."
        ),
        15,
        blocker=True,
    )
    add_check(
        "trace_propagation",
        integration.trace_propagation_signal,
        (
            "Audit logs with trace_id were found."
            if integration.trace_propagation_signal
            else "No audit log with non-empty trace_id was found."
        ),
        10,
    )
    add_check(
        "duplicate_event_signal",
        integration.duplicate_event_signal,
        (
            "Duplicate event audit signal is present."
            if integration.duplicate_event_signal
            else "No duplicate event audit signal found."
        ),
        10,
    )
    add_check(
        "ack_flow_signal",
        integration.ack_flow_signal,
        (
            "Ack flow is observable via acknowledged incidents or ack logs."
            if integration.ack_flow_signal
            else "Ack flow signal is absent."
        ),
        15,
        blocker=True,
    )
    add_check(
        "escalation_guard_signal",
        integration.escalation_guard_signal,
        (
            "Escalation signal exists and no non-open incident was escalated unexpectedly."
            if integration.escalation_guard_signal
            else "Escalation guard signal is not satisfied."
        ),
        10,
    )
    add_check(
        "dlq_reporting_signal",
        integration.dlq_reporting_signal,
        (
            "DLQ replay report is available."
            if integration.dlq_reporting_signal
            else "DLQ replay report is missing or empty."
        ),
        15,
        blocker=True,
    )

    heartbeat_checks = {
        "queue_metrics_snapshot": "queue_metrics_snapshot",
        "prune_dlq": "prune_dlq",
        "stale_incident_sweeper": "stale_incident_sweeper",
    }
    if "_error" in heartbeats:
        add_check(
            "heartbeat_access",
            False,
            f"Redis heartbeat snapshot unavailable: {heartbeats['_error'].get('error')}",
            15,
            blocker=True,
        )
    else:
        for task_name, label in heartbeat_checks.items():
            heartbeat = heartbeats.get(task_name, {})
            last_run_at = heartbeat.get("last_run_at")
            last_status = heartbeat.get("last_status")
            passed = bool(last_run_at) and last_status == "ok"
            details = (
                f"last_run_at={last_run_at}, last_status={last_status}"
                if passed
                else f"Missing or unhealthy heartbeat: last_run_at={last_run_at}, last_status={last_status}"
            )
            add_check(
                f"heartbeat:{label}",
                passed,
                details,
                8,
                blocker=True,
            )

    score = max(0, min(100, score))
    if blockers:
        status = "red"
    elif any(not check.passed for check in checks):
        status = "yellow"
    else:
        status = "green"

    evidence = {
        "integration": integration.evidence,
        "heartbeats": heartbeats,
    }

    return schemas.OpsReadinessResponse(
        score=score,
        status=typing_cast(typing_Literal["green", "yellow", "red"], status),
        checks=checks,
        blockers=blockers,
        evidence=evidence,
    )


@app.get("/ops/readiness", response_model=schemas.OpsReadinessResponse)
def get_ops_readiness(db: Session = Depends(get_db)):
    return _evaluate_ops_readiness(db)


@app.get("/dispatch/voice/twiml/{notification_id}")
@app.post("/dispatch/voice/twiml/{notification_id}")
def generate_twiml(notification_id: str, db: Session = Depends(get_db)):
    try:
        notification_uuid = uuid.UUID(notification_id)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail="Invalid notification id") from exc

    notification = (
        db.query(models.Notification)
        .filter(models.Notification.id == notification_uuid)
        .first()
    )
    if not notification:
        raise HTTPException(status_code=404, detail="Not found")

    incident = (
        db.query(models.Incident)
        .filter(models.Incident.id == notification.incident_id)
        .first()
    )
    if not incident:
        raise HTTPException(status_code=404, detail="Incident not found")

    trace_id = _get_or_create_trace_id(db, incident.id)

    # Generate TwiML
    twiml_response = f"""<?xml version="1.0" encoding="UTF-8"?>
<Response>
    <Say language="pt-BR" voice="Polly.Camila-Neural">
        Atenção. Você tem um incidente crítico da fonte {incident.source}. {incident.title}.
        Pressione 1 para reconhecer o erro e assumir o incidente.
    </Say>
    <Gather numDigits="1" action="{APP_BASE_URL}/dispatch/voice/callback/{notification_id}" method="POST" />
</Response>"""

    # Audit log
    audit = models.AuditLog(
        trace_id=trace_id,
        incident_id=incident.id,
        action=models.AuditAction.TWIML_GENERATED,
        details_json={"notification_id": notification_id},
    )
    db.add(audit)
    db.commit()

    return Response(content=twiml_response, media_type="application/xml")


@app.get("/dispatch/voice/twiml/prerecorded/{notification_id}")
@app.post("/dispatch/voice/twiml/prerecorded/{notification_id}")
def generate_prerecorded_twiml(notification_id: str, db: Session = Depends(get_db)):
    if not VOICE_PRERECORDED_AUDIO_URL:
        raise HTTPException(
            status_code=503, detail="Pre-recorded audio URL is not configured"
        )

    try:
        notification_uuid = uuid.UUID(notification_id)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail="Invalid notification id") from exc

    notification = (
        db.query(models.Notification)
        .filter(models.Notification.id == notification_uuid)
        .first()
    )
    if not notification:
        raise HTTPException(status_code=404, detail="Not found")

    incident = (
        db.query(models.Incident)
        .filter(models.Incident.id == notification.incident_id)
        .first()
    )
    if not incident:
        raise HTTPException(status_code=404, detail="Incident not found")

    trace_id = _get_or_create_trace_id(db, incident.id)
    twiml_response = f"""<?xml version="1.0" encoding="UTF-8"?>
<Response>
    <Play>{VOICE_PRERECORDED_AUDIO_URL}</Play>
    <Gather numDigits="1" action="{APP_BASE_URL}/dispatch/voice/callback/{notification_id}" method="POST" />
    <Say language="pt-BR">Se necessário, repita a ligação com a equipe de plantão.</Say>
</Response>"""

    db.add(
        models.AuditLog(
            trace_id=trace_id,
            incident_id=incident.id,
            action=models.AuditAction.TWIML_GENERATED,
            details_json={
                "notification_id": notification_id,
                "mode": "pre_recorded",
                "audio_url": VOICE_PRERECORDED_AUDIO_URL,
            },
        )
    )
    db.commit()
    return Response(content=twiml_response, media_type="application/xml")


@app.post("/dispatch/voice/callback/{notification_id}")
async def handle_voice_callback(
    notification_id: str,
    request: Request,
    db: Session = Depends(get_db),
    x_voice_timestamp: str | None = Header(default=None, alias="X-Voice-Timestamp"),
    x_voice_signature: str | None = Header(default=None, alias="X-Voice-Signature"),
):
    try:
        notification_uuid = uuid.UUID(notification_id)
    except ValueError:
        return {"status": "ignored"}

    raw_body = await request.body()
    signature_ok, signature_reason = _verify_voice_webhook_signature(
        raw_body,
        x_voice_timestamp,
        x_voice_signature,
    )

    form_data = await request.form()
    digits = form_data.get("Digits", "")

    notification = (
        db.query(models.Notification)
        .filter(models.Notification.id == notification_uuid)
        .first()
    )
    if not notification:
        return {"status": "ignored"}

    incident = (
        db.query(models.Incident)
        .filter(models.Incident.id == notification.incident_id)
        .with_for_update()
        .first()
    )
    if not incident:
        return {"status": "ignored"}

    trace_id = _get_or_create_trace_id(db, incident.id)
    db.add(
        models.AuditLog(
            trace_id=trace_id,
            incident_id=incident.id,
            action=models.AuditAction.CALLBACK_RECEIVED,
            details_json={
                "channel": "VOICE",
                "digits": digits,
                "notification_id": notification_id,
                "rejected": not signature_ok,
                "reason": signature_reason if not signature_ok else None,
            },
        )
    )
    if not signature_ok:
        db.commit()
        notify_event(
            "seriema.voice.callback.rejected",
            {
                "incident_id": str(incident.id),
                "notification_id": notification_id,
                "reason": signature_reason,
            },
            level="warning",
            trace_id=trace_id,
        )
        raise HTTPException(status_code=401, detail="Invalid voice webhook signature")

    if digits == "1" and incident.status == models.IncidentStatus.OPEN:
        incident.status = models.IncidentStatus.ACKNOWLEDGED
        incident.acknowledged_at = datetime.now(timezone.utc)
        incident.acknowledged_by = str(notification.contact_id)
        notification.status = models.NotificationStatus.ANSWERED_VOICE

        # Log resolution
        audit = models.AuditLog(
            trace_id=trace_id,
            incident_id=incident.id,
            action=models.AuditAction.ACK_RECEIVED,
            details_json={
                "channel": "VOICE",
                "digits": digits,
                "contact_id": str(notification.contact_id),
            },
        )
        db.add(audit)
        notify_event(
            "seriema.voice.callback.ack_received",
            {
                "incident_id": str(incident.id),
                "notification_id": notification_id,
                "contact_id": str(notification.contact_id),
            },
            trace_id=trace_id,
        )
    db.commit()

    # We could play a confirmation message here
    twiml_ack = """<?xml version="1.0" encoding="UTF-8"?><Response><Say language="pt-BR">Incidente reconhecido com sucesso. Obrigado.</Say></Response>"""
    return Response(content=twiml_ack, media_type="application/xml")
