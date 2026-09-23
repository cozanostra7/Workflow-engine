from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy import select
from sqlalchemy.orm import Session, selectinload

from app.db import get_session
from app.models import TaskRun, WorkflowRun
from app.schemas import TaskRunRead, WorkflowRunRead
from app.services.run_service import create_workflow_run

workflow_runs_router = APIRouter(prefix="/workflows", tags=["workflow runs"])
runs_router = APIRouter(prefix="/runs", tags=["workflow runs"])


@workflow_runs_router.post(
    "/{workflow_id}/runs",
    response_model=WorkflowRunRead,
    status_code=status.HTTP_201_CREATED,
)
def start_workflow_run(
    workflow_id: UUID, session: Session = Depends(get_session)
) -> WorkflowRun:
    run = create_workflow_run(session, workflow_id)
    if run is None:
        raise HTTPException(status_code=404, detail="workflow or workflow version not found")
    return run


@runs_router.get("/{run_id}", response_model=WorkflowRunRead)
def get_workflow_run(
    run_id: UUID, session: Session = Depends(get_session)
) -> WorkflowRun:
    run = session.get(WorkflowRun, run_id)
    if run is None:
        raise HTTPException(status_code=404, detail="workflow run not found")
    return run


@runs_router.get("/{run_id}/tasks", response_model=list[TaskRunRead])
def list_task_runs(
    run_id: UUID, session: Session = Depends(get_session)
) -> list[TaskRun]:
    run_exists = session.get(WorkflowRun, run_id)
    if run_exists is None:
        raise HTTPException(status_code=404, detail="workflow run not found")

    statement = (
        select(TaskRun)
        .where(TaskRun.workflow_run_id == run_id)
        .options(selectinload(TaskRun.workflow_step))
        .order_by(TaskRun.created_at, TaskRun.id)
    )
    task_runs = list(session.scalars(statement))
    task_runs.sort(key=lambda task: task.workflow_step.position)
    return task_runs
