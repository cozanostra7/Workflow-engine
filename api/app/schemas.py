from datetime import datetime
import uuid

from pydantic import BaseModel, ConfigDict, Field


class WorkflowStepCreate(BaseModel):
    name: str = Field(min_length=1, max_length=200, pattern=r"^[A-Za-z0-9_-]+$")
    type: str = Field(min_length=1, max_length=100)
    input: dict = Field(default_factory=dict)
    depends_on: list[str] = Field(default_factory=list)


class WorkflowCreate(BaseModel):
    name: str = Field(min_length=1, max_length=200)
    steps: list[WorkflowStepCreate] = Field(min_length=1)


class WorkflowStepRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    name: str
    type: str = Field(validation_alias="task_type")
    input: dict
    depends_on: list[str]


class WorkflowVersionRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    version: int
    created_at: datetime
    steps: list[WorkflowStepRead]


class WorkflowRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: uuid.UUID
    name: str
    created_at: datetime
    latest_version: WorkflowVersionRead
