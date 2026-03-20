import os
import sys
import uuid
from datetime import datetime, timezone

os.environ["DATABASE_URL"] = "postgresql://postgres:postgres@localhost:55432/eventsaas"
os.environ["REDIS_URL"] = "redis://localhost:56379/0"
os.environ["APP_BASE_URL"] = "http://testserver"

sys.path.insert(0, r"D:\Repo")

from fastapi.testclient import TestClient

from Seriema import models
from Seriema.database import SessionLocal
from Seriema.main import app
from Seriema.worker import celery_app, dispatch_incident, handle_escalation


def main() -> None:
    celery_app.conf.task_always_eager = True
    celery_app.conf.task_eager_propagates = True

    client = TestClient(app)
    db = SessionLocal()
    try:
        db.query(models.AuditLog).delete()
        db.query(models.Notification).delete()
        db.query(models.GroupMember).delete()
        db.query(models.Incident).delete()
        db.query(models.Rule).delete()
        db.query(models.Group).delete()
        db.query(models.Contact).delete()
        db.commit()

        group = models.Group(name=f"oncall-{uuid.uuid4().hex[:8]}", description="Grupo de oncall")
        contact = models.Contact(name="Alice", email="alice@example.com", phone="+5511999999999")
        db.add(group)
        db.add(contact)
        db.commit()
        db.refresh(group)
        db.refresh(contact)

        db.add(models.GroupMember(group_id=group.id, contact_id=contact.id))
        db.commit()

        rule = models.Rule(
            rule_name="critical-cpu",
            condition_json={"source": "prometheus", "severity": "CRITICAL"},
            recipient_group_id=group.id,
            channels=["voice"],
            requires_ack=False,
            ack_deadline=None,
            fallback_policy_json={"escalation_group_id": str(group.id), "channels": ["VOICE"]},
        )
        db.add(rule)
        db.commit()
        db.refresh(rule)

        event_payload = {
            "source": "prometheus",
            "external_event_id": f"evt-{uuid.uuid4().hex[:8]}",
            "severity": "CRITICAL",
            "title": "Database CPU > 90%",
            "message": "load spike",
            "payload_json": {"host": "db01", "cpu": 95},
            "dedupe_key": f"db01-cpu-{uuid.uuid4().hex[:6]}",
        }

        ingest = client.post("/events/incoming", json=event_payload)
        assert ingest.status_code == 200, ingest.text
        ingest_body = ingest.json()
        assert ingest_body["status"] == "accepted", ingest_body
        assert ingest_body["matched_rule"] is True, ingest_body
        incident_id = ingest_body["incident_id"]
        trace_id = ingest_body["trace_id"]

        incident = db.query(models.Incident).filter(models.Incident.id == incident_id).first()
        assert incident is not None
        notif = db.query(models.Notification).filter(models.Notification.incident_id == incident.id).first()
        assert notif is not None
        assert notif.status == models.NotificationStatus.SENT

        callback = client.post(f"/dispatch/voice/callback/{notif.id}", data={"Digits": "1"})
        assert callback.status_code == 200, callback.text
        db.refresh(incident)
        db.refresh(notif)
        if incident.status != models.IncidentStatus.ACKNOWLEDGED:
            logs = (
                db.query(models.AuditLog)
                .filter(models.AuditLog.incident_id == incident.id)
                .order_by(models.AuditLog.created_at.asc())
                .all()
            )
            print("DEBUG_CALLBACK", {"incident_status": str(incident.status), "notification_status": str(notif.status)})
            print("DEBUG_ACTIONS", [str(log.action) for log in logs])
            print("DEBUG_DETAILS", [log.details_json for log in logs])
        assert incident.status == models.IncidentStatus.ACKNOWLEDGED
        assert incident.acknowledged_at is not None
        assert notif.status == models.NotificationStatus.ANSWERED_VOICE

        duplicate = client.post("/events/incoming", json=event_payload)
        assert duplicate.status_code == 200
        duplicate_body = duplicate.json()
        assert duplicate_body["status"] == "ignored"
        assert duplicate_body["reason"] == "duplicate"

        before = db.query(models.Notification).filter(models.Notification.incident_id == incident.id).count()
        dispatch_incident(str(incident.id), trace_id)
        after = db.query(models.Notification).filter(models.Notification.incident_id == incident.id).count()
        assert before == after

        incident2 = models.Incident(
            external_event_id=f"evt-{uuid.uuid4().hex[:8]}",
            source="prometheus",
            severity="CRITICAL",
            title="Memory > 95%",
            message="new incident",
            payload_json={"host": "db02", "mem": 96},
            status=models.IncidentStatus.OPEN,
            matched_rule_id=rule.id,
            dedupe_key=f"db02-mem-{uuid.uuid4().hex[:6]}",
        )
        db.add(incident2)
        db.commit()
        db.refresh(incident2)

        handle_escalation(str(incident2.id), trace_id)
        db.refresh(incident2)
        assert incident2.status == models.IncidentStatus.ESCALATED

        actions = [
            x[0]
            for x in db.query(models.AuditLog.action)
            .filter(models.AuditLog.incident_id == incident.id)
            .all()
        ]
        assert models.AuditAction.EVENT_RECEIVED in actions
        assert models.AuditAction.RULE_MATCHED in actions
        assert models.AuditAction.TASK_QUEUED in actions
        assert models.AuditAction.NOTIFICATION_SENT in actions
        assert models.AuditAction.ACK_RECEIVED in actions

        print(
            {
                "ok": True,
                "incident_acknowledged_at": incident.acknowledged_at.isoformat(),
                "incident2_status": incident2.status.value,
                "duplicate_reason": duplicate_body["reason"],
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }
        )
    finally:
        db.close()


if __name__ == "__main__":
    main()
