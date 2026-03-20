import uuid
from datetime import datetime, timezone
from fastapi import FastAPI, Depends, HTTPException, Request, Response
from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError
from . import models, schemas
from .database import get_db
from .redis_client import is_duplicate
from .engine import evaluate_rules
from .worker import dispatch_incident
import os

app = FastAPI(title="Event SaaS API", version="0.1.0")

APP_BASE_URL = os.getenv("APP_BASE_URL", "https://api.event-saas.com")

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

@app.get("/health")
def health_check():
    return {"status": "ok"}

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
    active_rules = db.query(models.Rule).all()
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
        dispatch_incident.apply_async(args=(str(incident.id), audit_trace_id), queue="queue:dispatch")
        
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

@app.post("/contacts", response_model=schemas.ContactResponse)
def create_contact(contact: schemas.ContactCreate, db: Session = Depends(get_db)):
    db_contact = models.Contact(**contact.dict())
    db.add(db_contact)
    db.commit()
    db.refresh(db_contact)
    return db_contact

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
async def handle_voice_callback(notification_id: str, request: Request, db: Session = Depends(get_db)):
    try:
        notification_uuid = uuid.UUID(notification_id)
    except ValueError:
        return {"status": "ignored"}

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
            details_json={"channel": "VOICE", "digits": digits, "notification_id": notification_id},
        )
    )
    
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
