"""add metabase analytics views

Revision ID: 20260320_0004
Revises: 20260320_0003
Create Date: 2026-03-20 14:10:00
"""

from alembic import op
import os
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = "20260320_0004"
down_revision = "20260320_0003"
branch_labels = None
depends_on = None

DB_SCHEMA = os.getenv("SERIEMA_DB_SCHEMA", "seriema")


def upgrade() -> None:
    op.execute(
        sa.text(f"""
            CREATE OR REPLACE VIEW {DB_SCHEMA}.v_incident_sla AS
            SELECT
                i.id AS incident_id,
                i.source,
                i.severity,
                i.status,
                i.created_at,
                i.acknowledged_at,
                CASE
                    WHEN i.acknowledged_at IS NOT NULL THEN
                        EXTRACT(EPOCH FROM (i.acknowledged_at - i.created_at))
                    ELSE NULL
                END AS tta_seconds
            FROM {DB_SCHEMA}.incidents i
            """)
    )

    op.execute(
        sa.text(f"""
            CREATE OR REPLACE VIEW {DB_SCHEMA}.v_channel_delivery AS
            SELECT
                n.id AS notification_id,
                n.incident_id,
                n.channel,
                n.status,
                n.created_at,
                n.updated_at
            FROM {DB_SCHEMA}.notifications n
            """)
    )

    op.execute(
        sa.text(f"""
            CREATE OR REPLACE VIEW {DB_SCHEMA}.v_ops_summary_24h AS
            WITH recent_incidents AS (
                SELECT
                    i.id,
                    i.status,
                    i.acknowledged_at,
                    i.created_at
                FROM {DB_SCHEMA}.incidents i
                WHERE i.created_at >= (NOW() AT TIME ZONE 'UTC') - INTERVAL '24 hours'
            )
            SELECT
                COUNT(*) AS total_incidents_24h,
                COUNT(*) FILTER (WHERE status = 'OPEN') AS open_incidents_24h,
                COUNT(*) FILTER (WHERE status = 'ACKNOWLEDGED') AS acknowledged_incidents_24h,
                COUNT(*) FILTER (WHERE status = 'RESOLVED') AS resolved_incidents_24h,
                COUNT(*) FILTER (WHERE status = 'ESCALATED') AS escalated_incidents_24h,
                CASE
                    WHEN COUNT(*) > 0 THEN
                        ROUND(
                            COUNT(*) FILTER (WHERE acknowledged_at IS NOT NULL)::numeric / COUNT(*)::numeric,
                            4
                        )
                    ELSE 0
                END AS ack_rate
            FROM recent_incidents
            """)
    )


def downgrade() -> None:
    op.execute(sa.text(f"DROP VIEW IF EXISTS {DB_SCHEMA}.v_ops_summary_24h"))
    op.execute(sa.text(f"DROP VIEW IF EXISTS {DB_SCHEMA}.v_channel_delivery"))
    op.execute(sa.text(f"DROP VIEW IF EXISTS {DB_SCHEMA}.v_incident_sla"))
