import json
import uuid
import hashlib
import hmac
import os
import time
import re
from datetime import datetime, timezone, timedelta
from fastapi import FastAPI, Depends, HTTPException, Request, Response, Query, Header
from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError
from sqlalchemy import func, text
from . import models, schemas
from .database import get_db
from .redis_client import is_duplicate, redis_conn
from .engine import evaluate_rules
from .worker import dispatch_incident, replay_dlq, celery_app
from .config import (
    APP_BASE_URL,
    ALERT_ACK_RATE_WARN,
    ALERT_DLQ_CRIT,
    ALERT_DLQ_WARN,
    ALERT_QUEUE_WARN,
    DLQ_QUEUE_NAME,
    METRICS_KEY,
    OPS_ENDPOINT_MAX_LIMIT,
    VOICE_WEBHOOK_MAX_SKEW_SECONDS,
    queue_name,
    prefixed_redis_key,
)

VOICE_WEBHOOK_SECRET = os.environ.get("VOICE_WEBHOOK_SECRET")

app = FastAPI(title="Event SaaS API", version="0.1.0")

def _serialize_incident(incident: models.Incident) -> schemas.IncidentResponse:
    return schemas.IncidentResponse(
        id=incident.id,
        external_event_id=incident.external_event_id,
        source=incident.source,
        severity=incident.severity,
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

def _dependency_status(ok: bool, detail: str | None = None) -> schemas.DependencyStatusDetail:
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
    raw_entries = redis_conn.lrange(prefixed_redis_key(DLQ_QUEUE_NAME), 0, max(limit, 0) - 1)
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
        average_tta_seconds=float(average_tta_seconds) if average_tta_seconds is not None else None,
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

@app.post("/events/incoming", response_model=schemas.EventIngestResponse)
def ingest_event(event: schemas.EventIncoming, db: Session = Depends(get_db)):
    audit_trace_id = str(uuid.uuid4())

    if event.dedupe_key and is_duplicate(f"dedupe:{event.dedupe_key}"):
        db.add(
            models.AuditLog(
                trace_id=audit_trace_id,
                incident_id=None,
                action=models.AuditAction.DUPLICATED_EVENT,
                details_json={
                    "source": event.source,
                    "external_event_id": event.external_event_id,
                    "dedupe_key": event.dedupe_key,
                },
            )
        )
        db.commit()
        return schemas.EventIngestResponse(
            status="ignored",
            reason="duplicate",
            incident_id=None,
            matched_rule=False,
            trace_id=audit_trace_id,
        )
            
    event_dict = event.dict()
    active_rules = (
        db.query(models.Rule)
        .filter(models.Rule.active.is_(True))
        .order_by(models.Rule.priority.asc(), models.Rule.rule_name.asc())
        .all()
    )
    matched_rule = evaluate_rules(event_dict, active_rules)
    
    incident = models.Incident(
        external_event_id=event.external_event_id,
        source=event.source,
        severity=event.severity,
        title=event.title,
        message=event.message,
        payload_json=event.payload_json,
        status=models.IncidentStatus.OPEN,
        matched_rule_id=matched_rule.id if matched_rule else None,
        dedupe_key=event.dedupe_key
    )
    db.add(incident)
    try:
        db.commit()
    except IntegrityError:
        db.rollback()
        return schemas.EventIngestResponse(
            status="ignored",
            reason="duplicate_source_external_event_id",
            incident_id=None,
            matched_rule=False,
            trace_id=audit_trace_id,
        )
    db.refresh(incident)
    
    audit = models.AuditLog(
        trace_id=audit_trace_id,
        incident_id=incident.id,
        action=models.AuditAction.EVENT_RECEIVED,
        details_json={"payload": event_dict}
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
        
    return schemas.EventIngestResponse(
        status="accepted",
        incident_id=incident.id,
        matched_rule=bool(matched_rule),
        trace_id=audit_trace_id,
    )

@app.post("/rules", response_model=schemas.RuleResponse)
def create_rule(rule: schemas.RuleCreate, db: Session = Depends(get_db)):
    db_rule = models.Rule(**rule.dict())
    db.add(db_rule)
    db.commit()
    db.refresh(db_rule)
    return db_rule

@app.post("/groups", response_model=schemas.GroupResponse)
def create_group(group: schemas.GroupCreate, db: Session = Depends(get_db)):
    db_group = models.Group(**group.dict())
    db.add(db_group)
    db.commit()
    db.refresh(db_group)
    return db_group

@app.post("/contacts", response_model=schemas.ContactResponse)
def create_contact(contact: schemas.ContactCreate, db: Session = Depends(get_db)):
    db_contact = models.Contact(**contact.dict())
    db.add(db_contact)
    db.commit()
    db.refresh(db_contact)
    return db_contact

@app.post("/groups/{group_id}/members", response_model=schemas.GroupMemberResponse)
def add_group_member(group_id: str, member: schemas.GroupMemberCreate, db: Session = Depends(get_db)):
    try:
        group_uuid = uuid.UUID(group_id)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail="Invalid group id") from exc

    group = db.query(models.Group).filter(models.Group.id == group_uuid).first()
    if not group:
        raise HTTPException(status_code=404, detail="Group not found")

    contact = db.query(models.Contact).filter(models.Contact.id == member.contact_id).first()
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
        db_member = models.GroupMember(group_id=group_uuid, contact_id=member.contact_id)
        db.add(db_member)
        db.commit()

    return schemas.GroupMemberResponse(group_id=group_uuid, contact_id=member.contact_id)

@app.get("/groups/{group_id}/members", response_model=list[schemas.GroupMemberResponse])
def list_group_members(group_id: str, db: Session = Depends(get_db)):
    try:
        group_uuid = uuid.UUID(group_id)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail="Invalid group id") from exc

    group = db.query(models.Group).filter(models.Group.id == group_uuid).first()
    if not group:
        raise HTTPException(status_code=404, detail="Group not found")

    members = db.query(models.GroupMember).filter(models.GroupMember.group_id == group_uuid).all()
    return [
        schemas.GroupMemberResponse(group_id=member.group_id, contact_id=member.contact_id)
        for member in members
    ]

@app.get("/incidents/{incident_id}", response_model=schemas.IncidentDetailsResponse)
def get_incident(incident_id: str, db: Session = Depends(get_db)):
    try:
        incident_uuid = uuid.UUID(incident_id)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail="Invalid incident id") from exc

    incident = db.query(models.Incident).filter(models.Incident.id == incident_uuid).first()
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
        str(key.decode("utf-8") if isinstance(key, bytes) else key): _metric_value_from_redis(value)
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
        str(key.decode("utf-8") if isinstance(key, bytes) else key): _metric_value_from_redis(value)
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

@app.get("/dispatch/voice/twiml/{notification_id}")
@app.post("/dispatch/voice/twiml/{notification_id}")
def generate_twiml(notification_id: str, db: Session = Depends(get_db)):
    try:
        notification_uuid = uuid.UUID(notification_id)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail="Invalid notification id") from exc

    notification = db.query(models.Notification).filter(models.Notification.id == notification_uuid).first()
    if not notification:
        raise HTTPException(status_code=404, detail="Not found")
        
    incident = db.query(models.Incident).filter(models.Incident.id == notification.incident_id).first()
    if not incident:
        raise HTTPException(status_code=404, detail="Incident not found")

    trace_id = _get_or_create_trace_id(db, incident.id)
    
    # Generate TwiML
    twiml_response = f'''<?xml version="1.0" encoding="UTF-8"?>
<Response>
    <Say language="pt-BR" voice="Polly.Camila-Neural">
        Atenção. Você tem um incidente crítico da fonte {incident.source}. {incident.title}. 
        Pressione 1 para reconhecer o erro e assumir o incidente.
    </Say>
    <Gather numDigits="1" action="{APP_BASE_URL}/dispatch/voice/callback/{notification_id}" method="POST" />
</Response>'''
    
    # Audit log
    audit = models.AuditLog(
        trace_id=trace_id,
        incident_id=incident.id,
        action=models.AuditAction.TWIML_GENERATED,
        details_json={"notification_id": notification_id}
    )
    db.add(audit)
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
    
    notification = db.query(models.Notification).filter(models.Notification.id == notification_uuid).first()
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
            details_json={"channel": "VOICE", "digits": digits, "contact_id": str(notification.contact_id)}
        )
        db.add(audit)
    db.commit()
    
    # We could play a confirmation message here
    twiml_ack = '''<?xml version="1.0" encoding="UTF-8"?><Response><Say language="pt-BR">Incidente reconhecido com sucesso. Obrigado.</Say></Response>'''
    return Response(content=twiml_ack, media_type="application/xml")
