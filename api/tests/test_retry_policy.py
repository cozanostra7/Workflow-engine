import pytest

from app.services.retry_policy import retry_delay_seconds
from app.schemas import WorkflowStepCreate


def test_retry_delay_doubles_after_each_failed_attempt() -> None:
    assert [retry_delay_seconds(0.5, attempt) for attempt in range(1, 5)] == [
        0.5,
        1.0,
        2.0,
        4.0,
    ]


def test_retry_delay_is_capped() -> None:
    assert retry_delay_seconds(60, 4) == 300


@pytest.mark.parametrize("attempt", [0, -1])
def test_retry_delay_rejects_nonpositive_attempt_numbers(attempt: int) -> None:
    with pytest.raises(ValueError, match="failed_attempt"):
        retry_delay_seconds(1, attempt)


def test_retry_delay_rejects_negative_base_delay() -> None:
    with pytest.raises(ValueError, match="base_delay_seconds"):
        retry_delay_seconds(-1, 1)


@pytest.mark.parametrize("attempts", [0, 11])
def test_step_attempt_limit_is_validated(attempts: int) -> None:
    with pytest.raises(ValueError):
        WorkflowStepCreate(name="retry", type="echo", max_attempts=attempts)
