import uuid

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session, selectinload

from app.db import get_session
from app.models import Workflow, WorkflowStep, WorkflowVersion
from app.schemas import WorkflowCreate, WorkflowRead
from app.services.workflow_validation import validate_workflow_steps

router = APIRouter(prefix="/workflows", tags=["workflows"])


def _load_workflow(session: Session, workflow_id: uuid.UUID) -> Workflow | None:
    statement = (
        select(Workflow)
        .where(Workflow.id == workflow_id)
        .options(selectinload(Workflow.versions).selectinload(WorkflowVersion.steps))
    )
    workflow = session.scalar(statement)
    if workflow is not None:
        workflow.versions.sort(key=lambda version: version.version)
        for version in workflow.versions:
            version.steps.sort(key=lambda step: step.position)
    return workflow


@router.post("", response_model=WorkflowRead, status_code=status.HTTP_201_CREATED)
def create_workflow(
    payload: WorkflowCreate, session: Session = Depends(get_session)
) -> Workflow:
    try:
        validate_workflow_steps(payload.steps)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc

    workflow = Workflow(name=payload.name)
    version = WorkflowVersion(version=1)
    version.steps = [
        WorkflowStep(
            name=step.name,
            task_type=step.type,
            position=position,
            input=step.input,
            depends_on=step.depends_on,
            max_attempts=step.max_attempts,
            retry_backoff_seconds=step.retry_backoff_seconds,
        )
        for position, step in enumerate(payload.steps)
    ]
    workflow.versions.append(version)
    session.add(workflow)
    try:
        session.commit()
    except IntegrityError as exc:
        session.rollback()
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail={"code": "workflow_name_conflict", "message": "workflow name already exists"},
        ) from exc

    return _load_workflow(session, workflow.id)


@router.get("", response_model=list[WorkflowRead])
def list_workflows(session: Session = Depends(get_session)) -> list[Workflow]:
    statement = (
        select(Workflow)
        .options(selectinload(Workflow.versions).selectinload(WorkflowVersion.steps))
        .order_by(Workflow.created_at, Workflow.id)
    )
    workflows = list(session.scalars(statement).unique())
    for workflow in workflows:
        workflow.versions.sort(key=lambda version: version.version)
        for version in workflow.versions:
            version.steps.sort(key=lambda step: step.position)
    return workflows


@router.get("/{workflow_id}", response_model=WorkflowRead)
def get_workflow(
    workflow_id: uuid.UUID, session: Session = Depends(get_session)
) -> Workflow:
    workflow = _load_workflow(session, workflow_id)
    if workflow is None:
        raise HTTPException(status_code=404, detail="workflow not found")
    return workflow
