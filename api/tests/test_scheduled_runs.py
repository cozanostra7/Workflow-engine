from datetime import UTC, datetime, timedelta
import time
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import delete, select

from app.db import SessionLocal
from app.main import app
from app.models import TaskRun, Workflow, WorkflowRun, WorkflowVersion


@pytest.fixture
def workflow_definition():
    name = f"scheduled_test_{uuid4().hex}"
    with TestClient(app) as client:
        response = client.post(
            "/workflows",
            json={
                "name": name,
                "steps": [{"name": "echo_step", "type": "echo", "input": {"ok": True}}],
            },
        )
        assert response.status_code == 201
        yield client, response.json()

    with SessionLocal.begin() as session:
        workflow_ids = select(Workflow.id).where(Workflow.name == name)
        version_ids = select(WorkflowVersion.id).where(WorkflowVersion.workflow_id.in_(workflow_ids))
        run_ids = select(WorkflowRun.id).where(WorkflowRun.workflow_version_id.in_(version_ids))
        session.execute(delete(TaskRun).where(TaskRun.workflow_run_id.in_(run_ids)))
        session.execute(delete(WorkflowRun).where(WorkflowRun.id.in_(run_ids)))
        session.execute(delete(Workflow).where(Workflow.name == name))


def test_scheduled_run_waits_then_executes(workflow_definition) -> None:
    client, workflow = workflow_definition
    scheduled_for = datetime.now(UTC) + timedelta(seconds=3)

    response = client.post(
        f"/workflows/{workflow['id']}/runs",
        json={"scheduled_for": scheduled_for.isoformat()},
    )

    assert response.status_code == 201
    run = response.json()
    assert run["status"] == "pending"
    assert run["started_at"] is None
    assert datetime.fromisoformat(run["scheduled_for"].replace("Z", "+00:00")) == scheduled_for

    tasks = client.get(f"/runs/{run['id']}/tasks").json()
    assert tasks[0]["status"] == "queued"
    assert tasks[0]["attempt"] == 0
    assert tasks[0]["attempts"] == []

    deadline = time.monotonic() + 12
    while time.monotonic() < deadline:
        run = client.get(f"/runs/{run['id']}").json()
        if run["status"] == "completed":
            break
        time.sleep(0.1)

    assert run["status"] == "completed"
    started_at = datetime.fromisoformat(run["started_at"].replace("Z", "+00:00"))
    assert started_at >= scheduled_for


@pytest.mark.parametrize(
    "scheduled_for",
    [
        (datetime.now(UTC) - timedelta(seconds=1)).isoformat(),
        "2026-09-24T12:00:00",
    ],
)
def test_invalid_schedule_is_rejected(workflow_definition, scheduled_for: str) -> None:
    client, workflow = workflow_definition

    response = client.post(
        f"/workflows/{workflow['id']}/runs",
        json={"scheduled_for": scheduled_for},
    )

    assert response.status_code == 422


def test_swagger_schema_exposes_schedule_body(workflow_definition) -> None:
    client, _ = workflow_definition
    operation = client.get("/openapi.json").json()["paths"][
        "/workflows/{workflow_id}/runs"
    ]["post"]

    request_schema = operation["requestBody"]["content"]["application/json"]["schema"]

    assert request_schema == {"$ref": "#/components/schemas/WorkflowRunStart"}
