from datetime import datetime
import uuid

from pydantic import BaseModel, ConfigDict, Field, field_validator
from pydantic import AliasPath


class WorkflowStepCreate(BaseModel):
    name: str = Field(min_length=1, max_length=200, pattern=r"^[A-Za-z0-9_-]+$")
    type: str = Field(min_length=1, max_length=100)
    input: dict = Field(default_factory=dict)
    depends_on: list[str] = Field(default_factory=list)
    max_attempts: int = Field(default=1, ge=1, le=10)
    retry_backoff_seconds: float = Field(default=0.5, ge=0.1, le=60)


class WorkflowCreate(BaseModel):
    name: str = Field(min_length=1, max_length=200)
    steps: list[WorkflowStepCreate] = Field(min_length=1)


class WorkflowStepRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    name: str
    type: str = Field(validation_alias="task_type")
    input: dict
    depends_on: list[str]
    max_attempts: int
    retry_backoff_seconds: float


class WorkflowVersionRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    version: int
    created_at: datetime
    steps: list[WorkflowStepRead]


class WorkflowRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    name: str
    created_at: datetime
    latest_version: WorkflowVersionRead


class WorkflowRunRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    workflow_version_id: uuid.UUID
    status: str
    created_at: datetime
    scheduled_for: datetime | None
    started_at: datetime | None
    finished_at: datetime | None


class WorkflowRunStart(BaseModel):
    scheduled_for: datetime | None = None

    @field_validator("scheduled_for")
    @classmethod
    def scheduled_time_must_include_timezone(
        cls, value: datetime | None
    ) -> datetime | None:
        if value is not None and value.tzinfo is None:
            raise ValueError("scheduled_for must include a timezone")
        return value


class TaskAttemptRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    attempt_number: int
    status: str
    worker_id: str
    started_at: datetime
    finished_at: datetime | None
    output: dict | None
    error: dict | None


class TaskRunRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    workflow_step_id: uuid.UUID
    step_name: str = Field(validation_alias=AliasPath("workflow_step", "name"))
    task_type: str = Field(validation_alias=AliasPath("workflow_step", "task_type"))
    status: str
    attempt: int
    input: dict
    output: dict | None
    error: dict | None
    created_at: datetime
    started_at: datetime | None
    finished_at: datetime | None
    worker_id: str | None
    last_heartbeat_at: datetime | None
    lease_expires_at: datetime | None
    attempts: list[TaskAttemptRead]
