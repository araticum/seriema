"""add rule activation and priority

Revision ID: 20260320_0002
Revises: 20260320_0001
Create Date: 2026-03-20 13:30:00
"""

from alembic import op
import os
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = "20260320_0002"
down_revision = "20260320_0001"
branch_labels = None
depends_on = None

DB_SCHEMA = os.getenv("SERIEMA_DB_SCHEMA", "seriema")


def upgrade() -> None:
    op.execute(
        sa.text(
            f'ALTER TABLE "{DB_SCHEMA}".rules '
            "ADD COLUMN IF NOT EXISTS active BOOLEAN NOT NULL DEFAULT true"
        )
    )
    op.execute(
        sa.text(
            f'ALTER TABLE "{DB_SCHEMA}".rules '
            "ADD COLUMN IF NOT EXISTS priority INTEGER NOT NULL DEFAULT 100"
        )
    )


def downgrade() -> None:
    op.execute(
        sa.text(f'ALTER TABLE "{DB_SCHEMA}".rules DROP COLUMN IF EXISTS priority')
    )
    op.execute(sa.text(f'ALTER TABLE "{DB_SCHEMA}".rules DROP COLUMN IF EXISTS active'))
