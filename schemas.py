from datetime import datetime
from pydantic import BaseModel
from typing import List, Optional, Dict, Any
import uuid
from .models import IncidentStatus, NotificationStatus, NotificationChannel, AuditAction

class EventIncoming(BaseModel):
    source: str
    external_event_id: str
    severity: str
    title: str
    message: Optional[str] = None
    payload_json: Optional[Dict[str, Any]] = None
    schedule_at: Optional[str] = None
    dedupe_key: Optional[str] = None

class ContactCreate(BaseModel):
    name: str
    email: Optional[str] = None
    phone: Optional[str] = None
    whatsapp: Optional[str] = None
    telegram_id: Optional[str] = None

class ContactResponse(ContactCreate):
    id: uuid.UUID

class GroupCreate(BaseModel):
    name: str
    description: Optional[str] = None

class GroupResponse(GroupCreate):
    id: uuid.UUID

class RuleCreate(BaseModel):
    rule_name: str
    condition_json: Dict[str, Any]
    recipient_group_id: uuid.UUID
    channels: List[str]
    requires_ack: bool = False
    ack_deadline: Optional[int] = None
    fallback_policy_json: Optional[Dict[str, Any]] = None

class RuleResponse(RuleCreate):
    id: uuid.UUID

class EventIngestResponse(BaseModel):
    status: str
    incident_id: Optional[uuid.UUID] = None
    matched_rule: bool = False
    trace_id: str
    reason: Optional[str] = None

class IncidentResponse(BaseModel):
    id: uuid.UUID
    external_event_id: str
    source: str
    severity: str
    title: str
    message: Optional[str] = None
    payload_json: Optional[Dict[str, Any]] = None
    status: IncidentStatus
    matched_rule_id: Optional[uuid.UUID] = None
    dedupe_key: Optional[str] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    acknowledged_at: Optional[datetime] = None
    acknowledged_by: Optional[str] = None

class NotificationResponse(BaseModel):
    id: uuid.UUID
    incident_id: uuid.UUID
    contact_id: uuid.UUID
    channel: NotificationChannel
    status: NotificationStatus
    external_provider_id: Optional[str] = None
    error_message: Optional[str] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

class AuditLogResponse(BaseModel):
    id: uuid.UUID
    trace_id: str
    incident_id: Optional[uuid.UUID] = None
    action: AuditAction
    details_json: Optional[Dict[str, Any]] = None
    created_at: Optional[datetime] = None

class IncidentDetailsResponse(BaseModel):
    incident: IncidentResponse
    logs: List[AuditLogResponse]
