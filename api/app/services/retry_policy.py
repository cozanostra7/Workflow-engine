MAX_RETRY_BACKOFF_SECONDS = 300.0


def retry_delay_seconds(base_delay_seconds: float, failed_attempt: int) -> float:
    """Return capped exponential backoff after a failed attempt (attempts are 1-based)."""
    if failed_attempt < 1:
        raise ValueError("failed_attempt must be at least 1")
    if base_delay_seconds < 0:
        raise ValueError("base_delay_seconds cannot be negative")
    delay = base_delay_seconds * (2 ** (failed_attempt - 1))
    return min(delay, MAX_RETRY_BACKOFF_SECONDS)
