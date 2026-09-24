from datetime import UTC, datetime
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.orm import Session, selectinload

from app.models import TaskRun, Workflow, WorkflowRun, WorkflowVersion
from app.services.scheduler import schedule_ready_tasks


def create_workflow_run(
    session: Session,
    workflow_id: UUID,
    scheduled_for: datetime | None = None,
) -> WorkflowRun | None:
    """Create the run and all its initial task rows in one transaction."""
    if scheduled_for is not None:
        if scheduled_for.tzinfo is None:
            raise ValueError("scheduled_for must include a timezone")
        if scheduled_for <= datetime.now(UTC):
            raise ValueError("scheduled_for must be in the future")
    workflow = session.scalar(
        select(Workflow)
        .where(Workflow.id == workflow_id)
        .options(selectinload(Workflow.versions).selectinload(WorkflowVersion.steps))
    )
    if workflow is None:
        return None

    definition = max(workflow.versions, key=lambda version: version.version, default=None)
    if definition is None:
        return None

    run = WorkflowRun(
        workflow_version_id=definition.id,
        status="pending",
        scheduled_for=scheduled_for,
    )
    run.task_runs = [
        TaskRun(
            workflow_step_id=step.id,
            status="pending",
            attempt=0,
            input=step.input,
        )
        for step in sorted(definition.steps, key=lambda item: item.position)
    ]
    session.add(run)
    session.flush()
    schedule_ready_tasks(session, run)
    session.commit()
    session.refresh(run)
    return run
