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
from .worker import dispatch_incident, replay_dlq, celery_app, get_dlq_replay_report
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
    )

def _simulate_rule(rule: models.Rule, payload: dict[str, object]) -> schemas.RuleSimulationResponse:
    reasons: list[str] = []
    for key, expected_value in rule.condition_json.items():
        actual_value = payload.get(key, None)
        if actual_value != expected_value:
            reasons.append(
                f"{key}: expected {expected_value!r}, got {actual_value!r}"
            )

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

    rules = query.order_by(models.Rule.priority.asc(), models.Rule.rule_name.asc(), models.Rule.id.asc()).all()
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

def _load_contact_or_404(contact_id: str, db: Session) -> tuple[uuid.UUID, models.Contact]:
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
    if _has_any_rows(db.query(models.Notification).filter(models.Notification.contact_id == contact_uuid)):
        raise HTTPException(status_code=409, detail="Contact has related notifications")
    if _has_any_rows(db.query(models.GroupMember).filter(models.GroupMember.contact_id == contact_uuid)):
        raise HTTPException(status_code=409, detail="Contact has related group members")

def _delete_group_guard(db: Session, group_uuid: uuid.UUID) -> None:
    if _has_any_rows(db.query(models.Rule).filter(models.Rule.recipient_group_id == group_uuid)):
        raise HTTPException(status_code=409, detail="Group has related rules")
    if _has_any_rows(db.query(models.GroupMember).filter(models.GroupMember.group_id == group_uuid)):
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
    contacts = db.query(models.Contact).order_by(models.Contact.name.asc(), models.Contact.id.asc()).all()
    return schemas.ContactListResponse(
        total=len(contacts),
        limit=limit,
        offset=offset,
        items=[_serialize_contact(contact) for contact in contacts[offset : offset + limit]],
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
    update_data = {key: value for key, value in contact_update.model_dump().items() if value is not None}
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
    groups = db.query(models.Group).order_by(models.Group.name.asc(), models.Group.id.asc()).all()
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
    update_data = {key: value for key, value in group_update.model_dump().items() if value is not None}
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

@app.get("/incidents/{incident_id}/timeline", response_model=schemas.IncidentTimelineResponse)
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

    incident = db.query(models.Incident).filter(models.Incident.id == incident_uuid).first()
    if not incident:
        raise HTTPException(status_code=404, detail="Incident not found")

    _validate_ops_limit(limit)

    query = db.query(models.AuditLog).filter(models.AuditLog.incident_id == incident_uuid)
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

@app.post("/incidents/{incident_id}/ack", response_model=schemas.IncidentLifecycleResponse)
def acknowledge_incident(
    incident_id: str,
    payload: schemas.AckIncidentRequest,
    db: Session = Depends(get_db),
):
    try:
        incident_uuid = uuid.UUID(incident_id)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail="Invalid incident id") from exc

    incident = db.query(models.Incident).filter(models.Incident.id == incident_uuid).first()
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

    return schemas.IncidentLifecycleResponse(
        action="ack",
        incident=_serialize_incident(incident),
    )

@app.post("/incidents/{incident_id}/resolve", response_model=schemas.IncidentLifecycleResponse)
def resolve_incident(
    incident_id: str,
    payload: schemas.ResolveIncidentRequest,
    db: Session = Depends(get_db),
):
    try:
        incident_uuid = uuid.UUID(incident_id)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail="Invalid incident id") from exc

    incident = db.query(models.Incident).filter(models.Incident.id == incident_uuid).first()
    if not incident:
        raise HTTPException(status_code=404, detail="Incident not found")

    if incident.status not in {models.IncidentStatus.ACKNOWLEDGED, models.IncidentStatus.ESCALATED}:
        raise HTTPException(status_code=409, detail="Incident cannot be resolved from current status")

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

def _evaluate_ops_integration_status(db: Session) -> schemas.OpsIntegrationStatusResponse:
    evidence: dict[str, object] = {}

    enums_ok = all(
        hasattr(enum_cls, "__members__") and len(enum_cls.__members__) > 0
        for enum_cls in (models.IncidentStatus, models.NotificationStatus, models.AuditAction)
    )
    evidence["enums"] = {
        "incident_statuses": list(models.IncidentStatus.__members__.keys()),
        "notification_statuses": list(models.NotificationStatus.__members__.keys()),
        "audit_actions": list(models.AuditAction.__members__.keys()),
    }

    fallback_contract_ok = "fallback_policy_json" in schemas.RuleCreate.model_fields and "fallback_policy_json" in schemas.RuleUpdate.model_fields
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
    duplicate_events = [log for log in audit_logs if log.action == models.AuditAction.DUPLICATED_EVENT]
    ack_logs = [log for log in audit_logs if log.action == models.AuditAction.ACK_RECEIVED]
    acknowledged_incidents = [incident for incident in incidents if incident.status == models.IncidentStatus.ACKNOWLEDGED]
    escalated_logs = [log for log in audit_logs if log.action == models.AuditAction.ESCALATED]
    non_open_incidents = [incident for incident in incidents if incident.status != models.IncidentStatus.OPEN]
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
        str(key.decode("utf-8") if isinstance(key, bytes) else key): _coerce_redis_text(value)
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

    def add_check(name: str, passed: bool, details: str, weight: int, blocker: bool = False) -> None:
        nonlocal score
        checks.append(schemas.OpsReadinessCheck(name=name, passed=passed, details=details))
        if not passed:
            score -= weight
            if blocker:
                blockers.append(name)

    add_check(
        "integration_enums",
        integration.enums_ok,
        "Incident, notification and audit enums are available." if integration.enums_ok else "One or more enum classes are missing members.",
        20,
        blocker=True,
    )
    add_check(
        "fallback_contract",
        integration.fallback_contract_ok,
        "Rule fallback policy contract is available." if integration.fallback_contract_ok else "Rule fallback policy contract is unavailable.",
        15,
        blocker=True,
    )
    add_check(
        "trace_propagation",
        integration.trace_propagation_signal,
        "Audit logs with trace_id were found." if integration.trace_propagation_signal else "No audit log with non-empty trace_id was found.",
        10,
    )
    add_check(
        "duplicate_event_signal",
        integration.duplicate_event_signal,
        "Duplicate event audit signal is present." if integration.duplicate_event_signal else "No duplicate event audit signal found.",
        10,
    )
    add_check(
        "ack_flow_signal",
        integration.ack_flow_signal,
        "Ack flow is observable via acknowledged incidents or ack logs." if integration.ack_flow_signal else "Ack flow signal is absent.",
        15,
        blocker=True,
    )
    add_check(
        "escalation_guard_signal",
        integration.escalation_guard_signal,
        "Escalation signal exists and no non-open incident was escalated unexpectedly." if integration.escalation_guard_signal else "Escalation guard signal is not satisfied.",
        10,
    )
    add_check(
        "dlq_reporting_signal",
        integration.dlq_reporting_signal,
        "DLQ replay report is available." if integration.dlq_reporting_signal else "DLQ replay report is missing or empty.",
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
        status=status,
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
