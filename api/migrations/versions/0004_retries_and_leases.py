"""Add retry policy, attempt history, and task leases.

Revision ID: 0004_retries_and_leases
Revises: 0003_task_outbox
"""
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision = "0004_retries_and_leases"
down_revision = "0003_task_outbox"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "workflow_steps",
        sa.Column("max_attempts", sa.Integer(), server_default="1", nullable=False),
    )
    op.add_column(
        "workflow_steps",
        sa.Column("retry_backoff_seconds", sa.Float(), server_default="0.5", nullable=False),
    )
    op.alter_column("workflow_steps", "max_attempts", server_default=None)
    op.alter_column("workflow_steps", "retry_backoff_seconds", server_default=None)
    op.create_check_constraint(
        "ck_step_max_attempts_positive", "workflow_steps", "max_attempts >= 1"
    )
    op.create_check_constraint(
        "ck_step_retry_backoff_nonnegative", "workflow_steps", "retry_backoff_seconds >= 0"
    )

    op.add_column("task_runs", sa.Column("worker_id", sa.String(length=100), nullable=True))
    op.add_column("task_runs", sa.Column("last_heartbeat_at", sa.DateTime(timezone=True), nullable=True))
    op.add_column("task_runs", sa.Column("lease_expires_at", sa.DateTime(timezone=True), nullable=True))
    op.create_index("ix_task_runs_lease_expires_at", "task_runs", ["lease_expires_at"])

    op.drop_constraint("task_outbox_task_run_id_key", "task_outbox", type_="unique")
    op.add_column(
        "task_outbox",
        sa.Column("attempt", sa.Integer(), server_default="1", nullable=False),
    )
    op.add_column(
        "task_outbox",
        sa.Column("available_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
    )
    op.alter_column("task_outbox", "attempt", server_default=None)
    op.create_check_constraint("ck_task_outbox_attempt_positive", "task_outbox", "attempt > 0")
    op.create_unique_constraint("uq_task_outbox_attempt", "task_outbox", ["task_run_id", "attempt"])
    op.drop_index("ix_task_outbox_pending_created", table_name="task_outbox")
    op.create_index("ix_task_outbox_status_available", "task_outbox", ["status", "available_at"])

    op.create_table(
        "task_attempts",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("task_run_id", sa.Uuid(), nullable=False),
        sa.Column("attempt_number", sa.Integer(), nullable=False),
        sa.Column("status", sa.String(length=20), nullable=False),
        sa.Column("worker_id", sa.String(length=100), nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("finished_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("output", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.Column("error", postgresql.JSONB(astext_type=sa.Text()), nullable=True),
        sa.CheckConstraint("attempt_number > 0", name="ck_task_attempt_number_positive"),
        sa.CheckConstraint("status IN ('running','completed','failed','abandoned')", name="ck_task_attempt_status"),
        sa.ForeignKeyConstraint(["task_run_id"], ["task_runs.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("task_run_id", "attempt_number", name="uq_task_attempt_number"),
    )
    op.create_index("ix_task_attempts_task_run_id", "task_attempts", ["task_run_id"])


def downgrade() -> None:
    op.drop_index("ix_task_attempts_task_run_id", table_name="task_attempts")
    op.drop_table("task_attempts")
    op.drop_index("ix_task_outbox_status_available", table_name="task_outbox")
    op.create_index("ix_task_outbox_pending_created", "task_outbox", ["status", "created_at"])
    op.drop_constraint("uq_task_outbox_attempt", "task_outbox", type_="unique")
    op.drop_constraint("ck_task_outbox_attempt_positive", "task_outbox", type_="check")
    op.drop_column("task_outbox", "available_at")
    op.drop_column("task_outbox", "attempt")
    op.create_unique_constraint("task_outbox_task_run_id_key", "task_outbox", ["task_run_id"])
    op.drop_index("ix_task_runs_lease_expires_at", table_name="task_runs")
    op.drop_column("task_runs", "lease_expires_at")
    op.drop_column("task_runs", "last_heartbeat_at")
    op.drop_column("task_runs", "worker_id")
    op.drop_constraint("ck_step_retry_backoff_nonnegative", "workflow_steps", type_="check")
    op.drop_constraint("ck_step_max_attempts_positive", "workflow_steps", type_="check")
    op.drop_column("workflow_steps", "retry_backoff_seconds")
    op.drop_column("workflow_steps", "max_attempts")
