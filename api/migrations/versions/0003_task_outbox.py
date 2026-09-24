"""Add transactional task dispatch outbox.

Revision ID: 0003_task_outbox
Revises: 0002_step_position
"""
from alembic import op
import sqlalchemy as sa

revision = "0003_task_outbox"
down_revision = "0002_step_position"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "task_outbox",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("task_run_id", sa.Uuid(), nullable=False),
        sa.Column("status", sa.String(length=20), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("published_at", sa.DateTime(timezone=True), nullable=True),
        sa.CheckConstraint("status IN ('pending','published','cancelled')", name="ck_task_outbox_status"),
        sa.ForeignKeyConstraint(["task_run_id"], ["task_runs.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("task_run_id"),
    )
    op.create_index("ix_task_outbox_pending_created", "task_outbox", ["status", "created_at"])


def downgrade() -> None:
    op.drop_index("ix_task_outbox_pending_created", table_name="task_outbox")
    op.drop_table("task_outbox")
