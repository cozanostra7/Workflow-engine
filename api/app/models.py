import enum
import uuid
from datetime import datetime

from sqlalchemy import (
    CheckConstraint,
    DateTime,
    Enum,
    ForeignKey,
    Integer,
    String,
    UniqueConstraint,
    Uuid,
    func,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db import Base


class WorkflowStatus(str, enum.Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    PAUSED = "paused"


class TaskStatus(str, enum.Enum):
    PENDING = "pending"
    QUEUED = "queued"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    RETRYING = "retrying"
    CANCELLED = "cancelled"


class OutboxStatus(str, enum.Enum):
    PENDING = "pending"
    PUBLISHED = "published"
    CANCELLED = "cancelled"


class AttemptStatus(str, enum.Enum):
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    ABANDONED = "abandoned"


class Workflow(Base):
    __tablename__ = "workflows"

    id: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True, default=uuid.uuid4)
    name: Mapped[str] = mapped_column(String(200), nullable=False, unique=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False
    )

    versions: Mapped[list["WorkflowVersion"]] = relationship(
        back_populates="workflow", cascade="all, delete-orphan"
    )

    @property
    def latest_version(self) -> "WorkflowVersion | None":
        return max(self.versions, key=lambda item: item.version, default=None)


class WorkflowVersion(Base):
    __tablename__ = "workflow_versions"
    __table_args__ = (UniqueConstraint("workflow_id", "version", name="uq_workflow_version"),)

    id: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True, default=uuid.uuid4)
    workflow_id: Mapped[uuid.UUID] = mapped_column(
        Uuid, ForeignKey("workflows.id", ondelete="CASCADE"), nullable=False, index=True
    )
    version: Mapped[int] = mapped_column(Integer, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )

    workflow: Mapped[Workflow] = relationship(back_populates="versions")
    steps: Mapped[list["WorkflowStep"]] = relationship(
        back_populates="workflow_version", cascade="all, delete-orphan"
    )
    runs: Mapped[list["WorkflowRun"]] = relationship(back_populates="workflow_version")


class WorkflowStep(Base):
    __tablename__ = "workflow_steps"
    __table_args__ = (
        UniqueConstraint("workflow_version_id", "name", name="uq_workflow_step_name"),
        CheckConstraint("jsonb_typeof(depends_on) = 'array'", name="ck_step_dependencies_array"),
        CheckConstraint("max_attempts >= 1", name="ck_step_max_attempts_positive"),
        CheckConstraint("retry_backoff_seconds >= 0", name="ck_step_retry_backoff_nonnegative"),
    )

    id: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True, default=uuid.uuid4)
    workflow_version_id: Mapped[uuid.UUID] = mapped_column(
        Uuid, ForeignKey("workflow_versions.id", ondelete="CASCADE"), nullable=False, index=True
    )
    name: Mapped[str] = mapped_column(String(200), nullable=False)
    task_type: Mapped[str] = mapped_column(String(100), nullable=False)
    position: Mapped[int] = mapped_column(Integer, nullable=False)
    input: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)
    depends_on: Mapped[list[str]] = mapped_column(JSONB, nullable=False, default=list)
    max_attempts: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    retry_backoff_seconds: Mapped[float] = mapped_column(nullable=False, default=0.5)

    workflow_version: Mapped[WorkflowVersion] = relationship(back_populates="steps")
    task_runs: Mapped[list["TaskRun"]] = relationship(back_populates="workflow_step")


class WorkflowRun(Base):
    __tablename__ = "workflow_runs"
    __table_args__ = (CheckConstraint("status IN ('pending','running','completed','failed','cancelled','paused')", name="ck_workflow_run_status"),)

    id: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True, default=uuid.uuid4)
    workflow_version_id: Mapped[uuid.UUID] = mapped_column(
        Uuid, ForeignKey("workflow_versions.id", ondelete="RESTRICT"), nullable=False, index=True
    )
    status: Mapped[str] = mapped_column(String(20), nullable=False, default=WorkflowStatus.PENDING.value)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    scheduled_for: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    version: Mapped[int] = mapped_column(Integer, nullable=False, default=1)

    workflow_version: Mapped[WorkflowVersion] = relationship(back_populates="runs")
    task_runs: Mapped[list["TaskRun"]] = relationship(
        back_populates="workflow_run", cascade="all, delete-orphan"
    )


class TaskRun(Base):
    __tablename__ = "task_runs"
    __table_args__ = (
        UniqueConstraint("workflow_run_id", "workflow_step_id", name="uq_task_run_per_step"),
        CheckConstraint("status IN ('pending','queued','running','completed','failed','retrying','cancelled')", name="ck_task_run_status"),
        CheckConstraint("attempt >= 0", name="ck_task_run_attempt_nonnegative"),
    )

    id: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True, default=uuid.uuid4)
    workflow_run_id: Mapped[uuid.UUID] = mapped_column(
        Uuid, ForeignKey("workflow_runs.id", ondelete="CASCADE"), nullable=False, index=True
    )
    workflow_step_id: Mapped[uuid.UUID] = mapped_column(
        Uuid, ForeignKey("workflow_steps.id", ondelete="RESTRICT"), nullable=False
    )
    status: Mapped[str] = mapped_column(String(20), nullable=False, default=TaskStatus.PENDING.value)
    attempt: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    input: Mapped[dict] = mapped_column(JSONB, nullable=False, default=dict)
    output: Mapped[dict | None] = mapped_column(JSONB)
    error: Mapped[dict | None] = mapped_column(JSONB)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    started_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    worker_id: Mapped[str | None] = mapped_column(String(100))
    last_heartbeat_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    lease_expires_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), index=True)

    workflow_run: Mapped[WorkflowRun] = relationship(back_populates="task_runs")
    workflow_step: Mapped[WorkflowStep] = relationship(back_populates="task_runs")
    attempts: Mapped[list["TaskAttempt"]] = relationship(
        back_populates="task_run", cascade="all, delete-orphan", order_by="TaskAttempt.attempt_number"
    )
    outbox_entries: Mapped[list["TaskOutbox"]] = relationship(
        back_populates="task_run", cascade="all, delete-orphan"
    )


class TaskOutbox(Base):
    __tablename__ = "task_outbox"
    __table_args__ = (
        CheckConstraint("status IN ('pending','published','cancelled')", name="ck_task_outbox_status"),
        CheckConstraint("attempt > 0", name="ck_task_outbox_attempt_positive"),
        UniqueConstraint("task_run_id", "attempt", name="uq_task_outbox_attempt"),
    )

    id: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True, default=uuid.uuid4)
    task_run_id: Mapped[uuid.UUID] = mapped_column(
        Uuid, ForeignKey("task_runs.id", ondelete="CASCADE"), nullable=False
    )
    attempt: Mapped[int] = mapped_column(Integer, nullable=False, default=1)
    status: Mapped[str] = mapped_column(String(20), nullable=False, default=OutboxStatus.PENDING.value)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    available_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    published_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))

    task_run: Mapped[TaskRun] = relationship(back_populates="outbox_entries")


class TaskAttempt(Base):
    __tablename__ = "task_attempts"
    __table_args__ = (
        UniqueConstraint("task_run_id", "attempt_number", name="uq_task_attempt_number"),
        CheckConstraint("attempt_number > 0", name="ck_task_attempt_number_positive"),
        CheckConstraint("status IN ('running','completed','failed','abandoned')", name="ck_task_attempt_status"),
    )

    id: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True, default=uuid.uuid4)
    task_run_id: Mapped[uuid.UUID] = mapped_column(
        Uuid, ForeignKey("task_runs.id", ondelete="CASCADE"), nullable=False, index=True
    )
    attempt_number: Mapped[int] = mapped_column(Integer, nullable=False)
    status: Mapped[str] = mapped_column(String(20), nullable=False, default=AttemptStatus.RUNNING.value)
    worker_id: Mapped[str] = mapped_column(String(100), nullable=False)
    started_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)
    finished_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    output: Mapped[dict | None] = mapped_column(JSONB)
    error: Mapped[dict | None] = mapped_column(JSONB)

    task_run: Mapped[TaskRun] = relationship(back_populates="attempts")
