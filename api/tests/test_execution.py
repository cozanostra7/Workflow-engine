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


def test_flaky_succeeds_after_configured_failures() -> None:
    output, error = execute_task(
        "flaky", {"failures_before_success": 2}, attempt_number=3
    )

    assert output == {"succeeded_on_attempt": 3}
    assert error is None


@pytest.mark.parametrize("seconds", [-1, 301])
def test_sleep_rejects_out_of_range_durations(seconds: int) -> None:
    output, error = execute_task("sleep", {"seconds": seconds})

    assert output is None
    assert error["type"] == "ValueError"


def test_unknown_task_type_fails_explicitly() -> None:
    output, error = execute_task("not_registered", {})

    assert output is None
    assert error["type"] == "ValueError"


def test_flaky_fails_until_it_reaches_success_attempt() -> None:
    first_output, first_error = execute_task(
        "flaky", {"failures_before_success": 1}, attempt_number=1
    )
    second_output, second_error = execute_task(
        "flaky", {"failures_before_success": 1}, attempt_number=2
    )

    assert first_output is None
    assert first_error["message"] == "simulated failure on attempt 1"
    assert second_output == {"succeeded_on_attempt": 2}
    assert second_error is None
