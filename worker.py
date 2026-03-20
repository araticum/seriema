import os
from celery import Celery
from sqlalchemy.orm import Session
from .database import SessionLocal
from .models import (
    Incident,
    GroupMember,
    Notification,
    AuditLog,
    Rule,
    IncidentStatus,
    NotificationStatus,
    NotificationChannel,
    AuditAction,
)
from .redis_client import acquire_idempotency_key

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
celery_app = Celery("event_saas", broker=REDIS_URL, backend=REDIS_URL)

celery_app.conf.update(
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    timezone="UTC",
    enable_utc=True,
    task_routes={
        "dispatcher": {"queue": "queue:dispatch"},
        "voice_worker": {"queue": "queue:voice"},
        "escalation_worker": {"queue": "queue:escalation"},
    },
)

@celery_app.task(name="dispatcher")
def dispatch_incident(incident_id: str, incoming_trace_id: str):
    if not acquire_idempotency_key(f"idemp:dispatch:{incident_id}", ttl_seconds=24 * 3600):
        return

    db: Session = SessionLocal()
    try:
        incident = db.query(Incident).filter(Incident.id == incident_id).first()
        if not incident:
            return

        rule = db.query(Rule).filter(Rule.id == incident.matched_rule_id).first()
        
        if not rule:
            return
            
        group_members = db.query(GroupMember).filter(GroupMember.group_id == rule.recipient_group_id).all()
        channels = rule.channels
        
        # Cria as notificações para ser enviadas por cada worker de canal
        created_notifications = []
        for member in group_members:
            for channel in channels:
                normalized_channel = str(channel).upper()
                notif = Notification(
                    incident_id=incident.id,
                    contact_id=member.contact_id,
                    channel=NotificationChannel[normalized_channel]
                )
                db.add(notif)
                created_notifications.append(notif)
                
        db.commit()
        
        # Agora manda para as filas apropriadas
        for notif in created_notifications:
            # Audit log de envio pra fila
            audit = AuditLog(
                trace_id=incoming_trace_id,
                incident_id=incident.id,
                action=AuditAction.TASK_QUEUED,
                details_json={"channel": notif.channel, "notification_id": str(notif.id)}
            )
            db.add(audit)
            
            if notif.channel == NotificationChannel.VOICE:
                send_voice_call.apply_async(args=(str(notif.id), incoming_trace_id), queue="queue:voice")
            elif notif.channel == NotificationChannel.TELEGRAM:
                # TODO: implementar worker dedicado
                continue
            elif notif.channel == NotificationChannel.EMAIL:
                # TODO: implementar worker dedicado
                continue
                
        db.commit()
        
        # Se exige de fallback
        if rule.requires_ack and rule.ack_deadline:
            handle_escalation.apply_async(
                args=(incident_id, incoming_trace_id),
                countdown=rule.ack_deadline,
                queue="queue:escalation",
            )
            
    finally:
        db.close()

@celery_app.task(name="voice_worker")
def send_voice_call(notification_id: str, trace_id: str):
    if not acquire_idempotency_key(f"idemp:voice:{notification_id}", ttl_seconds=24 * 3600):
        return

    db: Session = SessionLocal()
    try:
        notif = db.query(Notification).filter(Notification.id == notification_id).first()
        if notif:
            # Integração fake com Twilio API Client
            # client.calls.create(to=contact.phone, from_="+1...", url=f"https://api.site.com/dispatch/voice/twiml/{notif.id}")
            notif.status = NotificationStatus.SENT
            
            audit = AuditLog(
                trace_id=trace_id,
                incident_id=notif.incident_id,
                action=AuditAction.NOTIFICATION_SENT,
                details_json={"channel": "VOICE", "notification_id": notification_id}
            )
            db.add(audit)
            db.commit()
    finally:
        db.close()

@celery_app.task(name="escalation_worker")
def handle_escalation(incident_id: str, trace_id: str):
    if not acquire_idempotency_key(f"idemp:escalation:{incident_id}", ttl_seconds=24 * 3600):
        return

    db: Session = SessionLocal()
    try:
        incident = db.query(Incident).filter(Incident.id == incident_id).first()
        if incident and incident.status == IncidentStatus.OPEN:
            # Significa que não deram ACK
            incident.status = IncidentStatus.ESCALATED
            audit = AuditLog(
                trace_id=trace_id,
                incident_id=incident.id,
                action=AuditAction.ESCALATED,
                details_json={"reason": "ACK deadline expired without acknowledgment."}
            )
            db.add(audit)
            db.commit()
            # Chama lógica de Fallback descrita nas regras
            rule = db.query(Rule).filter(Rule.id == incident.matched_rule_id).first()
            if rule and rule.fallback_policy_json:
                policy = rule.fallback_policy_json
                escalation_group_id = policy.get("escalation_group_id") or policy.get("escalation_group")
                fallback_channels = policy.get("channels", ["VOICE"])
                
                if escalation_group_id:
                    members = db.query(GroupMember).filter(GroupMember.group_id == escalation_group_id).all()
                    for m in members:
                        for ch in fallback_channels:
                            normalized = str(ch).upper()
                            notif = Notification(
                                incident_id=incident.id,
                                contact_id=m.contact_id,
                                channel=NotificationChannel[normalized],
                            )
                            db.add(notif)
                            db.commit()
                            
                            audit_2 = AuditLog(
                                trace_id=trace_id,
                                incident_id=incident.id,
                                action=AuditAction.FALLBACK_TASK_QUEUED,
                                details_json={"channel": ch, "notification_id": str(notif.id)}
                            )
                            db.add(audit_2)
                            db.commit()
                            
                            if normalized == "VOICE":
                                send_voice_call.apply_async(args=(str(notif.id), trace_id), queue="queue:voice")
    finally:
        db.close()
