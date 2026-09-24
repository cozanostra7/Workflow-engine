"""Add one-time scheduling to workflow runs.

Revision ID: 0005_scheduled_runs
Revises: 0004_retries_and_leases
"""
from alembic import op
import sqlalchemy as sa

revision = "0005_scheduled_runs"
down_revision = "0004_retries_and_leases"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "workflow_runs",
        sa.Column("scheduled_for", sa.DateTime(timezone=True), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("workflow_runs", "scheduled_for")
