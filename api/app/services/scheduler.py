from datetime import UTC, datetime
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.orm import Session, selectinload

from app.models import (
    OutboxStatus,
    TaskOutbox,
    TaskRun,
    TaskStatus,
    WorkflowRun,
    WorkflowStatus,
)
from app.services.task_queue import TaskQueue


def schedule_ready_tasks(session: Session, run: WorkflowRun) -> None:
    """Queue pending tasks only after all named dependencies completed."""
    task_runs = list(
        session.scalars(
            select(TaskRun)
            .where(TaskRun.workflow_run_id == run.id)
            .options(selectinload(TaskRun.workflow_step))
            .order_by(TaskRun.workflow_step_id)
        ).all()
    )
    by_name = {task.workflow_step.name: task for task in task_runs}
    for task in task_runs:
        if task.status != TaskStatus.PENDING.value:
            continue
        dependencies = task.workflow_step.depends_on
        if all(by_name[name].status == TaskStatus.COMPLETED.value for name in dependencies):
            task.status = TaskStatus.QUEUED.value
            session.add(TaskOutbox(task_run_id=task.id))

    if run.started_at is None:
        run.started_at = datetime.now(UTC)
    run.status = WorkflowStatus.RUNNING.value


def publish_pending_outbox(session: Session, queue: TaskQueue, batch_size: int = 50) -> int:
    """Publish outbox rows; duplicate stream messages are safe to process idempotently."""
    entries = list(
        session.scalars(
            select(TaskOutbox)
            .where(TaskOutbox.status == OutboxStatus.PENDING.value)
            .order_by(TaskOutbox.created_at, TaskOutbox.id)
            .with_for_update(skip_locked=True)
            .limit(batch_size)
        ).all()
    )
    now = datetime.now(UTC)
    for entry in entries:
        queue.publish(entry.task_run_id)
        entry.status = OutboxStatus.PUBLISHED.value
        entry.published_at = now
    session.commit()
    return len(entries)


def claim_task(session: Session, task_run_id: UUID, *, redelivered: bool) -> bool:
    task = session.scalar(
        select(TaskRun).where(TaskRun.id == task_run_id).with_for_update()
    )
    if task is None:
        return False
    if task.status == TaskStatus.RUNNING.value and redelivered:
        # The same consumer is reclaiming a message left pending by a prior process.
        task.status = TaskStatus.QUEUED.value
    if task.status != TaskStatus.QUEUED.value:
        return False

    task.status = TaskStatus.RUNNING.value
    task.attempt += 1
    task.started_at = datetime.now(UTC)
    session.commit()
    return True


def finish_task(
    session: Session,
    task_run_id: UUID,
    *,
    output: dict | None = None,
    error: dict | None = None,
) -> None:
    task = session.scalar(
        select(TaskRun)
        .where(TaskRun.id == task_run_id)
        .options(selectinload(TaskRun.workflow_step))
        .with_for_update()
    )
    if task is None or task.status != TaskStatus.RUNNING.value:
        return

    run = session.scalar(
        select(WorkflowRun).where(WorkflowRun.id == task.workflow_run_id).with_for_update()
    )
    if run is None:
        return

    now = datetime.now(UTC)
    task.finished_at = now
    task.output = output
    task.error = error
    if error is not None:
        task.status = TaskStatus.FAILED.value
        run.status = WorkflowStatus.FAILED.value
        run.finished_at = now
        pending_tasks = session.scalars(
            select(TaskRun).where(
                TaskRun.workflow_run_id == run.id,
                TaskRun.status.in_([TaskStatus.PENDING.value, TaskStatus.QUEUED.value]),
            )
        ).all()
        for pending in pending_tasks:
            pending.status = TaskStatus.CANCELLED.value
            if pending.outbox_entry is not None:
                pending.outbox_entry.status = OutboxStatus.CANCELLED.value
    else:
        task.status = TaskStatus.COMPLETED.value
        if run.status not in {
            WorkflowStatus.FAILED.value,
            WorkflowStatus.CANCELLED.value,
        }:
            schedule_ready_tasks(session, run)
            current_statuses = session.scalars(
                select(TaskRun.status).where(TaskRun.workflow_run_id == run.id)
            ).all()
            if current_statuses and all(
                state == TaskStatus.COMPLETED.value for state in current_statuses
            ):
                run.status = WorkflowStatus.COMPLETED.value
                run.finished_at = now
    session.commit()


def execute_task(task_type: str, task_input: dict) -> tuple[dict | None, dict | None]:
    """Execute one entry from the deliberately small local task registry."""
    try:
        if task_type == "echo":
            return task_input, None
        if task_type == "sleep":
            import time

            seconds = float(task_input.get("seconds", 1))
            if seconds < 0 or seconds > 30:
                raise ValueError("sleep seconds must be between 0 and 30")
            time.sleep(seconds)
            return {"slept_seconds": seconds}, None
        if task_type == "fail":
            raise RuntimeError(str(task_input.get("message", "requested task failure")))
        raise ValueError(f"unsupported task type '{task_type}'")
    except Exception as exc:
        return None, {"type": type(exc).__name__, "message": str(exc)}


def process_task(task_run_id: UUID, session: Session) -> tuple[dict | None, dict | None]:
    task = session.scalar(
        select(TaskRun)
        .where(TaskRun.id == task_run_id)
        .options(selectinload(TaskRun.workflow_step))
    )
    if task is None:
        return None, {"type": "TaskNotFound", "message": "task run no longer exists"}
    return execute_task(task.workflow_step.task_type, task.input or {})
