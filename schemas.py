from datetime import datetime
from pydantic import BaseModel, ConfigDict, Field
from pydantic import field_validator
from typing import List, Optional, Dict, Any, Literal
import uuid
from .models import IncidentStatus, NotificationStatus, NotificationChannel, AuditAction


def _validate_fallback_policy_json_value(value: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if value is None:
        return value
    if not isinstance(value, dict):
        raise ValueError("fallback_policy_json must be an object")

    missing_keys = [key for key in ("escalation_group_id", "channels") if key not in value]
    if missing_keys:
        raise ValueError("fallback_policy_json must include escalation_group_id and channels")

    escalation_group_id = value.get("escalation_group_id")
    try:
        uuid.UUID(str(escalation_group_id))
    except (TypeError, ValueError) as exc:
        raise ValueError("fallback_policy_json.escalation_group_id must be a valid UUID") from exc

    channels = value.get("channels")
    if not isinstance(channels, list) or not channels:
        raise ValueError("fallback_policy_json.channels must be a non-empty list of strings")
    if not all(isinstance(channel, str) for channel in channels):
        raise ValueError("fallback_policy_json.channels must be a non-empty list of strings")

    return value

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

class ContactUpdate(BaseModel):
    name: Optional[str] = None
    email: Optional[str] = None
    phone: Optional[str] = None
    whatsapp: Optional[str] = None
    telegram_id: Optional[str] = None

class ContactListResponse(BaseModel):
    total: int
    limit: int
    offset: int
    items: List[ContactResponse]

class GroupCreate(BaseModel):
    name: str
    description: Optional[str] = None

class GroupResponse(GroupCreate):
    id: uuid.UUID

class GroupUpdate(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None

class GroupListResponse(BaseModel):
    total: int
    limit: int
    offset: int
    items: List[GroupResponse]

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

    @field_validator("fallback_policy_json")
    @classmethod
    def validate_fallback_policy_json(cls, value):
        return _validate_fallback_policy_json_value(value)

class RuleResponse(RuleCreate):
    id: uuid.UUID

class RuleSimulationResponse(BaseModel):
    rule_id: uuid.UUID
    rule_name: str
    matched: bool
    reasons: List[str] = Field(default_factory=list)
    payload: Dict[str, Any] = Field(default_factory=dict)
    condition_json: Dict[str, Any] = Field(default_factory=dict)

class RuleUpdate(BaseModel):
    rule_name: Optional[str] = None
    condition_json: Optional[Dict[str, Any]] = None
    recipient_group_id: Optional[uuid.UUID] = None
    channels: Optional[List[str]] = None
    active: Optional[bool] = None
    priority: Optional[int] = None
    requires_ack: Optional[bool] = None
    ack_deadline: Optional[int] = None
    fallback_policy_json: Optional[Dict[str, Any]] = None

    @field_validator("fallback_policy_json")
    @classmethod
    def validate_fallback_policy_json(cls, value):
        return _validate_fallback_policy_json_value(value)

class RuleListResponse(BaseModel):
    total: int
    limit: int
    offset: int
    items: List[RuleResponse]

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

class IncidentTimelineResponse(BaseModel):
    total: int
    limit: int
    offset: int
    items: List[AuditLogResponse]

class IncidentListResponse(BaseModel):
    total: int
    limit: int
    offset: int
    items: List[IncidentResponse]

class AckIncidentRequest(BaseModel):
    acknowledged_by: Optional[str] = None

class ResolveIncidentRequest(BaseModel):
    resolved_by: Optional[str] = None
    note: Optional[str] = None

class IncidentLifecycleResponse(BaseModel):
    action: Literal["ack", "resolve"]
    incident: IncidentResponse

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

class DLQReplayReportResponse(BaseModel):
    model_config = ConfigDict(extra="allow")

    status: str
    started_at: Optional[str] = None
    finished_at: Optional[str] = None
    requested_limit: Optional[int] = None
    effective_limit: Optional[int] = None
    replayed: int = 0
    remaining: int = 0
    dry_run: bool = False
    locked: bool = False

class OpsIntegrationStatusResponse(BaseModel):
    enums_ok: bool
    fallback_contract_ok: bool
    trace_propagation_signal: bool
    duplicate_event_signal: bool
    ack_flow_signal: bool
    escalation_guard_signal: bool
    dlq_reporting_signal: bool
    evidence: Dict[str, Any] = Field(default_factory=dict)

class OpsReadinessCheck(BaseModel):
    name: str
    passed: bool
    details: str

class OpsReadinessResponse(BaseModel):
    score: int
    status: Literal["green", "yellow", "red"]
    checks: List[OpsReadinessCheck]
    blockers: List[str] = Field(default_factory=list)
    evidence: Dict[str, Any] = Field(default_factory=dict)

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
