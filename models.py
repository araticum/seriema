import uuid
from enum import Enum
from sqlalchemy import Column, String, Integer, DateTime, Boolean, ForeignKey, JSON, UniqueConstraint, Enum as SQLEnum
from sqlalchemy.dialects.postgresql import UUID, JSONB
from sqlalchemy.sql import func
from .database import Base

class IncidentStatus(str, Enum):
    OPEN = "OPEN"
    ACKNOWLEDGED = "ACKNOWLEDGED"
    RESOLVED = "RESOLVED"
    ESCALATED = "ESCALATED"

class NotificationChannel(str, Enum):
    VOICE = "VOICE"
    EMAIL = "EMAIL"
    WHATSAPP = "WHATSAPP"
    TELEGRAM = "TELEGRAM"
    CALENDAR = "CALENDAR"

class NotificationStatus(str, Enum):
    PENDING = "PENDING"
    SENT = "SENT"
    FAILED = "FAILED"
    DELIVERED = "DELIVERED"
    READ = "READ"
    ANSWERED_VOICE = "ANSWERED_VOICE"

class AuditAction(str, Enum):
    EVENT_RECEIVED = "EVENT_RECEIVED"
    DUPLICATED_EVENT = "DUPLICATED_EVENT"
    RULE_MATCHED = "RULE_MATCHED"
    TASK_QUEUED = "TASK_QUEUED"
    NOTIFICATION_SENT = "NOTIFICATION_SENT"
    CALLBACK_RECEIVED = "CALLBACK_RECEIVED"
    ACK_RECEIVED = "ACK_RECEIVED"
    ESCALATED = "ESCALATED"
    TWIML_GENERATED = "TWIML_GENERATED"
    FALLBACK_TASK_QUEUED = "FALLBACK_TASK_QUEUED"

class Contact(Base):
    __tablename__ = "contacts"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    name = Column(String, nullable=False)
    email = Column(String, nullable=True)
    phone = Column(String, nullable=True)
    whatsapp = Column(String, nullable=True)
    telegram_id = Column(String, nullable=True)

class Group(Base):
    __tablename__ = "groups"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    name = Column(String, nullable=False, unique=True)
    description = Column(String, nullable=True)

class GroupMember(Base):
    __tablename__ = "group_members"
    
    group_id = Column(UUID(as_uuid=True), ForeignKey("groups.id"), primary_key=True)
    contact_id = Column(UUID(as_uuid=True), ForeignKey("contacts.id"), primary_key=True)

class Rule(Base):
    __tablename__ = "rules"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    rule_name = Column(String, nullable=False)
    condition_json = Column(JSONB, nullable=False)  # e.g., {"source": "prometheus", "severity": "CRITICAL"}
    recipient_group_id = Column(UUID(as_uuid=True), ForeignKey("groups.id"), nullable=False)
    channels = Column(JSON, nullable=False)  # list of strings: ['voice', 'telegram', 'email']
    requires_ack = Column(Boolean, default=False)
    ack_deadline = Column(Integer, nullable=True)  # in seconds
    fallback_policy_json = Column(JSONB, nullable=True)

class Incident(Base):
    __tablename__ = "incidents"
    __table_args__ = (
        UniqueConstraint("source", "external_event_id", name="uq_incidents_source_external_event_id"),
    )
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    external_event_id = Column(String, nullable=False, index=True)
    source = Column(String, nullable=False)
    severity = Column(String, nullable=False)
    title = Column(String, nullable=False)
    message = Column(String, nullable=True)
    payload_json = Column(JSONB, nullable=True)
    status = Column(SQLEnum(IncidentStatus, name="incident_status"), nullable=False, default=IncidentStatus.OPEN)
    matched_rule_id = Column(UUID(as_uuid=True), ForeignKey("rules.id"), nullable=True)
    dedupe_key = Column(String, nullable=True, index=True)
    
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())
    acknowledged_at = Column(DateTime(timezone=True), nullable=True)
    acknowledged_by = Column(String, nullable=True)

class Notification(Base):
    __tablename__ = "notifications"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    incident_id = Column(UUID(as_uuid=True), ForeignKey("incidents.id"), nullable=False)
    contact_id = Column(UUID(as_uuid=True), ForeignKey("contacts.id"), nullable=False)
    channel = Column(SQLEnum(NotificationChannel, name="notification_channel"), nullable=False)
    status = Column(SQLEnum(NotificationStatus, name="notification_status"), nullable=False, default=NotificationStatus.PENDING)
    external_provider_id = Column(String, nullable=True)
    error_message = Column(String, nullable=True)
    
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now())

class AuditLog(Base):
    __tablename__ = "audit_logs"
    
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    trace_id = Column(String, nullable=False, index=True)
    incident_id = Column(UUID(as_uuid=True), ForeignKey("incidents.id"), nullable=True)
    action = Column(SQLEnum(AuditAction, name="audit_action"), nullable=False)
    details_json = Column(JSONB, nullable=True)
    
    created_at = Column(DateTime(timezone=True), server_default=func.now())
