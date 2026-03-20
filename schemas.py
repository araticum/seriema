from datetime import datetime
from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any, Literal
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

class GroupMemberCreate(BaseModel):
    contact_id: uuid.UUID

class GroupMemberResponse(BaseModel):
    group_id: uuid.UUID
    contact_id: uuid.UUID

class RuleCreate(BaseModel):
    rule_name: str
    condition_json: Dict[str, Any]
    recipient_group_id: uuid.UUID
    channels: List[str]
    active: bool = True
    priority: int = 100
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

class IncidentListResponse(BaseModel):
    total: int
    limit: int
    offset: int
    items: List[IncidentResponse]

class IncidentStatusCount(BaseModel):
    status: IncidentStatus
    count: int

class SLAMetricsResponse(BaseModel):
    hours: int
    window_start: datetime
    window_end: datetime
    total_incidents: int
    acknowledged_incidents: int
    acknowledgement_rate: float
    average_tta_seconds: Optional[float] = None
    incidents_by_status: List[IncidentStatusCount]

class QueueMetricsResponse(BaseModel):
    dispatch: int
    voice: int
    telegram: int
    email: int
    escalation: int
    dlq: int

class DependencyStatusDetail(BaseModel):
    status: str
    detail: Optional[str] = None

class HealthDepsResponse(BaseModel):
    postgres: DependencyStatusDetail
    redis: DependencyStatusDetail
    overall: str

class OpsMetricsResponse(BaseModel):
    redis_key: str
    metrics: Dict[str, Any]

class DLQPreviewItem(BaseModel):
    task_name: str
    trace_id: Optional[str] = None
    notification_id: Optional[str] = None
    incident_id: Optional[str] = None
    error: Optional[str] = None
    args: List[Any] = Field(default_factory=list)
    kwargs: Dict[str, Any] = Field(default_factory=dict)
    failed_at: Optional[str] = None

class DLQPreviewResponse(BaseModel):
    limit: int
    total_items: int
    items: List[DLQPreviewItem]

class DLQReplayResponse(BaseModel):
    status: str
    limit: int
    result: Optional[Dict[str, Any]] = None
    task_id: Optional[str] = None

class OperationalAlert(BaseModel):
    alert_type: str
    severity: Literal["info", "warn", "critical"]
    message: str
    actual: Optional[float] = None
    threshold: Optional[float] = None
    metadata: Dict[str, Any] = Field(default_factory=dict)

class OpsAlertsResponse(BaseModel):
    overall_severity: Literal["ok", "info", "warn", "critical"]
    alert_count: int
    alerts: List[OperationalAlert]
    metrics: Dict[str, Any] = Field(default_factory=dict)
    queue_metrics: QueueMetricsResponse
