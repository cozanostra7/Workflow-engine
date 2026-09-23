from uuid import uuid4

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import delete, select

from app.db import SessionLocal
from app.main import app
from app.models import TaskRun, Workflow, WorkflowRun, WorkflowVersion


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
                    {"name": "second", "type": "sleep", "depends_on": ["first"]},
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
    assert run["status"] == "pending"
    assert run["workflow_version_id"] == workflow["latest_version"]["id"]

    tasks_response = client.get(f"/runs/{run['id']}/tasks")
    assert tasks_response.status_code == 200
    tasks = tasks_response.json()
    assert [task["step_name"] for task in tasks] == ["first", "second"]
    assert [task["status"] for task in tasks] == ["pending", "pending"]
    assert tasks[0]["input"] == {"value": 1}


def test_get_run_returns_persisted_run(workflow_with_run) -> None:
    client, workflow = workflow_with_run
    created = client.post(f"/workflows/{workflow['id']}/runs")

    response = client.get(f"/runs/{created.json()['id']}")

    assert response.status_code == 200
    assert response.json()["id"] == created.json()["id"]
    assert response.json()["workflow_version_id"] == workflow["latest_version"]["id"]


def test_starting_unknown_workflow_returns_404(workflow_with_run) -> None:
    client, _ = workflow_with_run

    response = client.post(f"/workflows/{uuid4()}/runs")

    assert response.status_code == 404
