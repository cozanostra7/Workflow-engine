from app.models import TaskStatus, WorkflowStatus


def test_workflow_statuses_match_supported_state_vocabulary() -> None:
    assert {status.value for status in WorkflowStatus} == {
        "pending", "running", "completed", "failed", "cancelled", "paused"
    }


def test_task_statuses_match_supported_state_vocabulary() -> None:
    assert {status.value for status in TaskStatus} == {
        "pending", "queued", "running", "completed", "failed", "retrying", "cancelled"
    }
