import pytest

from app.services.scheduler import execute_task


def test_echo_returns_its_input() -> None:
    payload = {"message": "hello"}

    output, error = execute_task("echo", payload)

    assert output == payload
    assert error is None


def test_sleep_returns_elapsed_duration() -> None:
    output, error = execute_task("sleep", {"seconds": 0})

    assert output == {"slept_seconds": 0}
    assert error is None


def test_fail_returns_structured_error() -> None:
    output, error = execute_task("fail", {"message": "deliberate"})

    assert output is None
    assert error == {"type": "RuntimeError", "message": "deliberate"}


@pytest.mark.parametrize("seconds", [-1, 31])
def test_sleep_rejects_out_of_range_durations(seconds: int) -> None:
    output, error = execute_task("sleep", {"seconds": seconds})

    assert output is None
    assert error["type"] == "ValueError"


def test_unknown_task_type_fails_explicitly() -> None:
    output, error = execute_task("not_registered", {})

    assert output is None
    assert error["type"] == "ValueError"
