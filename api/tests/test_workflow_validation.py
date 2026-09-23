import pytest
from pydantic import ValidationError

from app.schemas import WorkflowStepCreate
from app.services.workflow_validation import validate_workflow_steps


def step(name: str, depends_on: list[str] | None = None) -> WorkflowStepCreate:
    return WorkflowStepCreate(name=name, type="echo", depends_on=depends_on or [])


def test_accepts_sequential_and_parallel_dags() -> None:
    validate_workflow_steps(
        [step("start"), step("left", ["start"]), step("right", ["start"]), step("join", ["left", "right"])]
    )


def test_rejects_duplicate_step_names() -> None:
    with pytest.raises(ValueError, match="step names must be unique"):
        validate_workflow_steps([step("same"), step("same")])


def test_rejects_unknown_dependency() -> None:
    with pytest.raises(ValueError, match="unknown step 'missing'"):
        validate_workflow_steps([step("one", ["missing"])])


def test_rejects_dependency_cycle() -> None:
    with pytest.raises(ValueError, match="contain a cycle"):
        validate_workflow_steps([step("one", ["two"]), step("two", ["one"])])


def test_rejects_duplicate_dependency_names() -> None:
    with pytest.raises(ValueError, match="duplicate dependencies"):
        validate_workflow_steps([step("start"), step("end", ["start", "start"])])


def test_step_names_reject_unsupported_characters() -> None:
    with pytest.raises(ValidationError):
        step("bad step name")
