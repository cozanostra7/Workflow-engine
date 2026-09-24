from datetime import UTC, datetime, timedelta
from uuid import UUID

from sqlalchemy import select, update
from sqlalchemy.orm import Session, selectinload

from app.models import (
    AttemptStatus,
    OutboxStatus,
    TaskAttempt,
    TaskOutbox,
    TaskRun,
    TaskStatus,
    WorkflowRun,
    WorkflowStatus,
)
from app.services.retry_policy import retry_delay_seconds
from app.services.task_queue import TaskQueue


def schedule_ready_tasks(session: Session, run: WorkflowRun) -> None:
    """Queue pending tasks only after all named dependencies completed."""
    task_runs = list(
        session.scalars(
            select(TaskRun)
            .where(TaskRun.workflow_run_id == run.id)
            .options(selectinload(TaskRun.workflow_step))
        ).all()
    )
    by_name = {task.workflow_step.name: task for task in task_runs}
    for task in task_runs:
        if task.status != TaskStatus.PENDING.value:
            continue
        if all(
            by_name[name].status == TaskStatus.COMPLETED.value
            for name in task.workflow_step.depends_on
        ):
            task.status = TaskStatus.QUEUED.value
            session.add(
                TaskOutbox(task_run_id=task.id, attempt=task.attempt + 1)
            )

    if run.started_at is None:
        run.started_at = datetime.now(UTC)
    run.status = WorkflowStatus.RUNNING.value


def publish_pending_outbox(session: Session, queue: TaskQueue, batch_size: int = 50) -> int:
    """Publish due dispatch intents; repeats are filtered by attempt number in PostgreSQL."""
    now = datetime.now(UTC)
    entries = list(
        session.scalars(
            select(TaskOutbox)
            .where(
                TaskOutbox.status == OutboxStatus.PENDING.value,
                TaskOutbox.available_at <= now,
            )
            .options(selectinload(TaskOutbox.task_run))
            .order_by(TaskOutbox.available_at, TaskOutbox.created_at, TaskOutbox.id)
            .with_for_update(skip_locked=True)
            .limit(batch_size)
        ).all()
    )
    for entry in entries:
        queue.publish(entry.task_run_id, entry.attempt)
        entry.status = OutboxStatus.PUBLISHED.value
        entry.published_at = now
        if entry.task_run.status == TaskStatus.RETRYING.value:
            entry.task_run.status = TaskStatus.QUEUED.value
    session.commit()
    return len(entries)


def claim_task(
    session: Session,
    task_run_id: UUID,
    target_attempt: int,
    worker_id: str,
    lease_seconds: float,
) -> bool:
    task = session.scalar(
        select(TaskRun)
        .where(TaskRun.id == task_run_id)
        .options(selectinload(TaskRun.attempts))
        .with_for_update()
    )
    if task is None:
        return False
    if task.status != TaskStatus.QUEUED.value or target_attempt != task.attempt + 1:
        return False

    now = datetime.now(UTC)
    task.status = TaskStatus.RUNNING.value
    task.attempt = target_attempt
    task.worker_id = worker_id
    task.started_at = task.started_at or now
    task.last_heartbeat_at = now
    task.lease_expires_at = now + timedelta(seconds=lease_seconds)
    task.attempts.append(
        TaskAttempt(
            attempt_number=target_attempt,
            worker_id=worker_id,
            status=AttemptStatus.RUNNING.value,
            started_at=now,
        )
    )
    session.commit()
    return True


def heartbeat_task(
    session: Session, task_run_id: UUID, worker_id: str, lease_seconds: float
) -> bool:
    now = datetime.now(UTC)
    result = session.execute(
        update(TaskRun)
        .where(
            TaskRun.id == task_run_id,
            TaskRun.status == TaskStatus.RUNNING.value,
            TaskRun.worker_id == worker_id,
        )
        .values(last_heartbeat_at=now, lease_expires_at=now + timedelta(seconds=lease_seconds))
    )
    session.commit()
    return result.rowcount == 1


def _schedule_retry(session: Session, task: TaskRun, now: datetime) -> None:
    delay = retry_delay_seconds(task.workflow_step.retry_backoff_seconds, task.attempt)
    task.status = TaskStatus.RETRYING.value
    task.finished_at = None
    task.worker_id = None
    task.lease_expires_at = None
    task.last_heartbeat_at = None
    session.add(
        TaskOutbox(
            task_run_id=task.id,
            attempt=task.attempt + 1,
            available_at=now + timedelta(seconds=delay),
        )
    )


def _fail_run(session: Session, run: WorkflowRun, now: datetime) -> None:
    run.status = WorkflowStatus.FAILED.value
    run.finished_at = now
    waiting = session.scalars(
        select(TaskRun).where(
            TaskRun.workflow_run_id == run.id,
            TaskRun.status.in_(
                [TaskStatus.PENDING.value, TaskStatus.QUEUED.value, TaskStatus.RETRYING.value]
            ),
        )
    ).all()
    for pending in waiting:
        pending.status = TaskStatus.CANCELLED.value
        pending.finished_at = now
    if waiting:
        session.execute(
            update(TaskOutbox)
            .where(
                TaskOutbox.task_run_id.in_([task.id for task in waiting]),
                TaskOutbox.status == OutboxStatus.PENDING.value,
            )
            .values(status=OutboxStatus.CANCELLED.value)
        )


def finish_task(
    session: Session,
    task_run_id: UUID,
    worker_id: str,
    *,
    output: dict | None = None,
    error: dict | None = None,
) -> None:
    task = session.scalar(
        select(TaskRun)
        .where(TaskRun.id == task_run_id)
        .options(selectinload(TaskRun.workflow_step), selectinload(TaskRun.attempts))
        .with_for_update()
    )
    if task is None or task.status != TaskStatus.RUNNING.value or task.worker_id != worker_id:
        return

    run = session.scalar(
        select(WorkflowRun).where(WorkflowRun.id == task.workflow_run_id).with_for_update()
    )
    if run is None:
        return

    now = datetime.now(UTC)
    attempt = next(
        (item for item in task.attempts if item.attempt_number == task.attempt), None
    )
    if attempt is None:
        raise RuntimeError("running task has no matching attempt record")
    attempt.finished_at = now
    attempt.output = output
    attempt.error = error
    task.output = output
    task.error = error
    task.worker_id = None
    task.last_heartbeat_at = None
    task.lease_expires_at = None

    if error is not None and task.attempt < task.workflow_step.max_attempts:
        attempt.status = AttemptStatus.FAILED.value
        _schedule_retry(session, task, now)
    elif error is not None:
        attempt.status = AttemptStatus.FAILED.value
        task.status = TaskStatus.FAILED.value
        task.finished_at = now
        _fail_run(session, run, now)
    else:
        attempt.status = AttemptStatus.COMPLETED.value
        task.status = TaskStatus.COMPLETED.value
        task.finished_at = now
        if run.status not in {WorkflowStatus.FAILED.value, WorkflowStatus.CANCELLED.value}:
            schedule_ready_tasks(session, run)
            task_statuses = session.scalars(
                select(TaskRun.status).where(TaskRun.workflow_run_id == run.id)
            ).all()
            if task_statuses and all(
                state == TaskStatus.COMPLETED.value for state in task_statuses
            ):
                run.status = WorkflowStatus.COMPLETED.value
                run.finished_at = now
    session.commit()


def reap_expired_leases(session: Session, *, now: datetime | None = None) -> int:
    """Abandon expired attempts and retry or fail their tasks."""
    current_time = now or datetime.now(UTC)
    expired = list(
        session.scalars(
            select(TaskRun)
            .where(
                TaskRun.status == TaskStatus.RUNNING.value,
                TaskRun.lease_expires_at <= current_time,
            )
            .options(selectinload(TaskRun.workflow_step), selectinload(TaskRun.attempts))
            .with_for_update(skip_locked=True)
        ).all()
    )
    for task in expired:
        run = session.scalar(
            select(WorkflowRun).where(WorkflowRun.id == task.workflow_run_id).with_for_update()
        )
        if run is None:
            continue
        lease_error = {"type": "LeaseExpired", "message": "worker heartbeat lease expired"}
        attempt = next(
            (item for item in task.attempts if item.attempt_number == task.attempt), None
        )
        if attempt is not None:
            attempt.status = AttemptStatus.ABANDONED.value
            attempt.finished_at = current_time
            attempt.error = lease_error
        task.error = lease_error
        task.worker_id = None
        task.last_heartbeat_at = None
        task.lease_expires_at = None
        if run.status in {WorkflowStatus.FAILED.value, WorkflowStatus.CANCELLED.value}:
            task.status = TaskStatus.CANCELLED.value
            task.finished_at = current_time
            continue
        if task.attempt < task.workflow_step.max_attempts:
            _schedule_retry(session, task, current_time)
        else:
            task.status = TaskStatus.FAILED.value
            task.finished_at = current_time
            _fail_run(session, run, current_time)
    session.commit()
    return len(expired)


def execute_task(
    task_type: str, task_input: dict, attempt_number: int = 1
) -> tuple[dict | None, dict | None]:
    """Execute one entry from the local task registry."""
    try:
        if task_type == "echo":
            return task_input, None
        if task_type == "sleep":
            import time

            seconds = float(task_input.get("seconds", 1))
            if seconds < 0 or seconds > 300:
                raise ValueError("sleep seconds must be between 0 and 300")
            time.sleep(seconds)
            return {"slept_seconds": seconds}, None
        if task_type == "fail":
            raise RuntimeError(str(task_input.get("message", "requested task failure")))
        if task_type == "flaky":
            failures = int(task_input.get("failures_before_success", 1))
            if failures < 0:
                raise ValueError("failures_before_success cannot be negative")
            if attempt_number <= failures:
                raise RuntimeError(f"simulated failure on attempt {attempt_number}")
            return {"succeeded_on_attempt": attempt_number}, None
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
    return execute_task(task.workflow_step.task_type, task.input or {}, task.attempt)
