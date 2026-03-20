"""initial schema

Revision ID: 20260320_0001
Revises:
Create Date: 2026-03-20 12:40:00
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


# revision identifiers, used by Alembic.
revision = "20260320_0001"
down_revision = None
branch_labels = None
depends_on = None


incident_status = sa.Enum("OPEN", "ACKNOWLEDGED", "RESOLVED", "ESCALATED", name="incident_status")
notification_channel = sa.Enum("VOICE", "EMAIL", "WHATSAPP", "TELEGRAM", "CALENDAR", name="notification_channel")
notification_status = sa.Enum(
    "PENDING",
    "SENT",
    "FAILED",
    "DELIVERED",
    "READ",
    "ANSWERED_VOICE",
    name="notification_status",
)
audit_action = sa.Enum(
    "EVENT_RECEIVED",
    "DUPLICATED_EVENT",
    "RULE_MATCHED",
    "TASK_QUEUED",
    "NOTIFICATION_SENT",
    "CALLBACK_RECEIVED",
    "ACK_RECEIVED",
    "ESCALATED",
    "TWIML_GENERATED",
    "FALLBACK_TASK_QUEUED",
    name="audit_action",
)
incident_status_ref = postgresql.ENUM(name="incident_status", create_type=False)
notification_channel_ref = postgresql.ENUM(name="notification_channel", create_type=False)
notification_status_ref = postgresql.ENUM(name="notification_status", create_type=False)
audit_action_ref = postgresql.ENUM(name="audit_action", create_type=False)


def upgrade() -> None:
    bind = op.get_bind()
    incident_status.create(bind, checkfirst=True)
    notification_channel.create(bind, checkfirst=True)
    notification_status.create(bind, checkfirst=True)
    audit_action.create(bind, checkfirst=True)

    op.create_table(
        "contacts",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("name", sa.String(), nullable=False),
        sa.Column("email", sa.String(), nullable=True),
        sa.Column("phone", sa.String(), nullable=True),
        sa.Column("whatsapp", sa.String(), nullable=True),
        sa.Column("telegram_id", sa.String(), nullable=True),
        sa.PrimaryKeyConstraint("id"),
    )

    op.create_table(
        "groups",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("name", sa.String(), nullable=False),
        sa.Column("description", sa.String(), nullable=True),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("name"),
    )

    op.create_table(
        "rules",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("rule_name", sa.String(), nullable=False),
        sa.Column("condition_json", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("recipient_group_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("channels", sa.JSON(), nullable=False),
        sa.Column("requires_ack", sa.Boolean(), nullable=True),
        sa.Column("ack_deadline", sa.Integer(), nullable=True),
        sa.Column("fallback_policy_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.ForeignKeyConstraint(["recipient_group_id"], ["groups.id"]),
        sa.PrimaryKeyConstraint("id"),
    )

    op.create_table(
        "incidents",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("external_event_id", sa.String(), nullable=False),
        sa.Column("source", sa.String(), nullable=False),
        sa.Column("severity", sa.String(), nullable=False),
        sa.Column("title", sa.String(), nullable=False),
        sa.Column("message", sa.String(), nullable=True),
        sa.Column("payload_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("status", incident_status_ref, nullable=False, server_default="OPEN"),
        sa.Column("matched_rule_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("dedupe_key", sa.String(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=True),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("acknowledged_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("acknowledged_by", sa.String(), nullable=True),
        sa.ForeignKeyConstraint(["matched_rule_id"], ["rules.id"]),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("source", "external_event_id", name="uq_incidents_source_external_event_id"),
    )
    op.create_index(op.f("ix_incidents_external_event_id"), "incidents", ["external_event_id"], unique=False)
    op.create_index(op.f("ix_incidents_dedupe_key"), "incidents", ["dedupe_key"], unique=False)

    op.create_table(
        "group_members",
        sa.Column("group_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("contact_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.ForeignKeyConstraint(["contact_id"], ["contacts.id"]),
        sa.ForeignKeyConstraint(["group_id"], ["groups.id"]),
        sa.PrimaryKeyConstraint("group_id", "contact_id"),
    )

    op.create_table(
        "notifications",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("incident_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("contact_id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("channel", notification_channel_ref, nullable=False),
        sa.Column("status", notification_status_ref, nullable=False, server_default="PENDING"),
        sa.Column("external_provider_id", sa.String(), nullable=True),
        sa.Column("error_message", sa.String(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=True),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=True),
        sa.ForeignKeyConstraint(["contact_id"], ["contacts.id"]),
        sa.ForeignKeyConstraint(["incident_id"], ["incidents.id"]),
        sa.PrimaryKeyConstraint("id"),
    )

    op.create_table(
        "audit_logs",
        sa.Column("id", postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column("trace_id", sa.String(), nullable=False),
        sa.Column("incident_id", postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column("action", audit_action_ref, nullable=False),
        sa.Column("details_json", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.text("now()"), nullable=True),
        sa.ForeignKeyConstraint(["incident_id"], ["incidents.id"]),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(op.f("ix_audit_logs_trace_id"), "audit_logs", ["trace_id"], unique=False)


def downgrade() -> None:
    op.drop_index(op.f("ix_audit_logs_trace_id"), table_name="audit_logs")
    op.drop_table("audit_logs")

    op.drop_table("notifications")
    op.drop_table("group_members")

    op.drop_index(op.f("ix_incidents_dedupe_key"), table_name="incidents")
    op.drop_index(op.f("ix_incidents_external_event_id"), table_name="incidents")
    op.drop_table("incidents")

    op.drop_table("rules")
    op.drop_table("groups")
    op.drop_table("contacts")

    bind = op.get_bind()
    audit_action.drop(bind, checkfirst=True)
    notification_status.drop(bind, checkfirst=True)
    notification_channel.drop(bind, checkfirst=True)
    incident_status.drop(bind, checkfirst=True)
