from uuid import uuid4
from datetime import UTC, datetime, timedelta

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import delete, select, update

from app.db import SessionLocal
from app.main import app
from app.models import (
    AttemptStatus,
    OutboxStatus,
    TaskAttempt,
    TaskOutbox,
    TaskRun,
    TaskStatus,
    Workflow,
    WorkflowRun,
    WorkflowStep,
    WorkflowVersion,
)
from app.services.scheduler import reap_expired_leases


def wait_for_terminal_run(client: TestClient, run_id: str) -> dict:
    import time

    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        run = client.get(f"/runs/{run_id}").json()
        if run["status"] in {"completed", "failed"}:
            return run
        time.sleep(0.05)
    raise AssertionError(f"run {run_id} did not reach a terminal state")


@pytest.fixture
def workflow_with_run():
    name = f"run_test_{uuid4().hex}"
    with TestClient(app) as client:
        created = client.post(
            "/workflows",
            json={
                "name": name,
                "steps": [
                    {"name": "first", "type": "echo", "input": {"value": 1}},
                    {"name": "second", "type": "sleep", "input": {"seconds": 0.01}, "depends_on": ["first"]},
                ],
            },
        )
        assert created.status_code == 201
        workflow = created.json()
        yield client, workflow

    with SessionLocal.begin() as session:
        workflow_ids = select(Workflow.id).where(Workflow.name == name)
        version_ids = select(WorkflowVersion.id).where(WorkflowVersion.workflow_id.in_(workflow_ids))
        run_ids = select(WorkflowRun.id).where(WorkflowRun.workflow_version_id.in_(version_ids))
        session.execute(delete(TaskRun).where(TaskRun.workflow_run_id.in_(run_ids)))
        session.execute(delete(WorkflowRun).where(WorkflowRun.id.in_(run_ids)))
        session.execute(delete(Workflow).where(Workflow.name == name))


def test_start_run_creates_pending_task_runs_in_step_order(workflow_with_run) -> None:
    client, workflow = workflow_with_run
    response = client.post(f"/workflows/{workflow['id']}/runs")

    assert response.status_code == 201
    run = response.json()
    assert run["status"] in {"running", "completed"}
    assert run["workflow_version_id"] == workflow["latest_version"]["id"]

    run = wait_for_terminal_run(client, run["id"])
    assert run["status"] == "completed"

    tasks_response = client.get(f"/runs/{run['id']}/tasks")
    assert tasks_response.status_code == 200
    tasks = tasks_response.json()
    assert [task["step_name"] for task in tasks] == ["first", "second"]
    assert [task["status"] for task in tasks] == ["completed", "completed"]
    assert tasks[0]["input"] == {"value": 1}


def test_get_run_returns_persisted_run(workflow_with_run) -> None:
    client, workflow = workflow_with_run
    created = client.post(f"/workflows/{workflow['id']}/runs")
    wait_for_terminal_run(client, created.json()["id"])

    response = client.get(f"/runs/{created.json()['id']}")

    assert response.status_code == 200
    assert response.json()["id"] == created.json()["id"]
    assert response.json()["workflow_version_id"] == workflow["latest_version"]["id"]


def test_task_failure_fails_run_and_cancels_dependent_steps(workflow_with_run) -> None:
    client, workflow = workflow_with_run
    with SessionLocal.begin() as session:
        session.execute(
            update(WorkflowStep)
            .where(
                WorkflowStep.workflow_version_id == workflow["latest_version"]["id"],
                WorkflowStep.name == "first",
            )
            .values(task_type="fail", input={"message": "deliberate test failure"})
        )

    created = client.post(f"/workflows/{workflow['id']}/runs")
    run = wait_for_terminal_run(client, created.json()["id"])
    tasks = client.get(f"/runs/{run['id']}/tasks").json()

    assert run["status"] == "failed"
    assert [task["status"] for task in tasks] == ["failed", "cancelled"]
    assert tasks[0]["error"]["message"] == "deliberate test failure"


def test_flaky_task_retries_then_unblocks_dependent_step(workflow_with_run) -> None:
    client, workflow = workflow_with_run
    with SessionLocal.begin() as session:
        session.execute(
            update(WorkflowStep)
            .where(
                WorkflowStep.workflow_version_id == workflow["latest_version"]["id"],
                WorkflowStep.name == "first",
            )
            .values(
                task_type="flaky",
                input={"failures_before_success": 1},
                max_attempts=2,
                retry_backoff_seconds=0.1,
            )
        )

    created = client.post(f"/workflows/{workflow['id']}/runs")
    run = wait_for_terminal_run(client, created.json()["id"])
    tasks = client.get(f"/runs/{run['id']}/tasks").json()

    assert run["status"] == "completed"
    assert tasks[0]["attempt"] == 2
    assert [attempt["status"] for attempt in tasks[0]["attempts"]] == [
        "failed",
        "completed",
    ]
    assert tasks[0]["output"] == {"succeeded_on_attempt": 2}
    assert tasks[1]["status"] == "completed"


def test_expired_lease_abandons_attempt_and_schedules_retry(workflow_with_run) -> None:
    _, workflow = workflow_with_run
    now = datetime.now(UTC)
    with SessionLocal.begin() as session:
        step = session.scalar(
            select(WorkflowStep).where(
                WorkflowStep.workflow_version_id == workflow["latest_version"]["id"],
                WorkflowStep.name == "first",
            )
        )
        step.max_attempts = 2
        step.retry_backoff_seconds = 60
        run = WorkflowRun(workflow_version_id=workflow["latest_version"]["id"], status="running")
        task = TaskRun(
            workflow_step_id=step.id,
            status=TaskStatus.RUNNING.value,
            attempt=1,
            input={},
            worker_id="worker-that-disappeared",
            started_at=now - timedelta(seconds=90),
            last_heartbeat_at=now - timedelta(seconds=60),
            lease_expires_at=now - timedelta(seconds=30),
        )
        task.attempts.append(
            TaskAttempt(
                attempt_number=1,
                status=AttemptStatus.RUNNING.value,
                worker_id="worker-that-disappeared",
                started_at=now - timedelta(seconds=90),
            )
        )
        run.task_runs.append(task)
        session.add(run)
        session.flush()
        task_id = task.id

    with SessionLocal() as session:
        reap_expired_leases(session, now=now)

    with SessionLocal() as session:
        task = session.get(TaskRun, task_id)
        assert task is not None
        assert task.attempts[0].status == AttemptStatus.ABANDONED.value
        outbox = session.scalar(
            select(TaskOutbox).where(
                TaskOutbox.task_run_id == task_id,
                TaskOutbox.attempt == 2,
            )
        )
        assert outbox is not None
        assert outbox.status == OutboxStatus.PENDING.value
        assert outbox.available_at > now


def test_starting_unknown_workflow_returns_404(workflow_with_run) -> None:
    client, _ = workflow_with_run

    response = client.post(f"/workflows/{uuid4()}/runs")

    assert response.status_code == 404
