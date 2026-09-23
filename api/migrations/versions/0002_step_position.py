"""Preserve declared workflow step order.

Revision ID: 0002_step_position
Revises: 0001_initial
"""
from alembic import op
import sqlalchemy as sa

revision = "0002_step_position"
down_revision = "0001_initial"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "workflow_steps",
        sa.Column("position", sa.Integer(), server_default="0", nullable=False),
    )
    op.alter_column("workflow_steps", "position", server_default=None)


def downgrade() -> None:
    op.drop_column("workflow_steps", "position")
