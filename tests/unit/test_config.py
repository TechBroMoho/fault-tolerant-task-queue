"""Settings validation and env loading (pure logic)."""

import pytest
from pydantic import ValidationError

from ftq.config import Settings


def test_env_vars_use_ftq_prefix(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("FTQ_QUEUE", "emails")
    monkeypatch.setenv("FTQ_CONCURRENCY", "3")
    s = Settings()
    assert (s.queue, s.concurrency) == ("emails", 3)
    # Explicit values beat env vars (the CLI's flag overrides rely on this).
    assert Settings(queue="reports").queue == "reports"


@pytest.mark.parametrize("name", ["", "has space", "brace{", "}", "x" * 65])
def test_queue_name_must_be_hash_tag_safe(name: str) -> None:
    with pytest.raises(ValidationError):
        Settings(queue=name)


def test_block_must_be_shorter_than_socket_timeout() -> None:
    # Otherwise every idle XREADGROUP BLOCK would hit the socket timeout.
    with pytest.raises(ValidationError, match="block_ms"):
        Settings(block_ms=5000, socket_timeout=5.0)
    Settings(block_ms=4999, socket_timeout=5.0)


def test_backoff_base_cannot_exceed_cap() -> None:
    with pytest.raises(ValidationError):
        Settings(retry_backoff_base=2.0, retry_backoff_cap=1.0)


def test_negative_values_rejected() -> None:
    for field in ("concurrency", "done_ttl_seconds", "shutdown_grace", "retry_attempts"):
        with pytest.raises(ValidationError):
            Settings.model_validate({field: -1})


def test_heartbeat_at_most_half_the_lease() -> None:
    # At least two heartbeats per lease, so one lost beat doesn't expire a healthy job.
    with pytest.raises(ValidationError, match="heartbeat_interval"):
        Settings(visibility_timeout=10.0, heartbeat_interval=5.1)
    Settings(visibility_timeout=10.0, heartbeat_interval=5.0)


def test_job_backoff_base_cannot_exceed_cap() -> None:
    with pytest.raises(ValidationError, match="job_backoff_base"):
        Settings(job_backoff_base=10.0, job_backoff_cap=5.0)


def test_consumer_prune_idle_must_exceed_block() -> None:
    with pytest.raises(ValidationError, match="consumer_prune_idle"):
        Settings(block_ms=1000, consumer_prune_idle=1.0)
    Settings(block_ms=1000, consumer_prune_idle=1.5)


def test_done_ttl_must_outlast_redelivery_bound() -> None:
    """ADR-010: a done/ledger key that expired while copies of the job can still be
    delivered would let a late copy commit (and apply its effect) a second time."""
    s = Settings(
        max_attempts=2,
        max_deliveries=3,
        visibility_timeout=10.0,
        heartbeat_interval=2.0,
        job_backoff_cap=20.0,
        done_ttl_seconds=0,  # 0 = never expire, always allowed
    )
    # 2 attempts x (3 deliveries x 10 s lease + 20 s backoff) = 100 s.
    assert s.job_lifetime_bound == 100.0
    base = s.model_dump()
    with pytest.raises(ValidationError, match="ADR-010"):
        Settings.model_validate({**base, "done_ttl_seconds": 999})  # < 10 x 100 s
    Settings.model_validate({**base, "done_ttl_seconds": 1000})


def test_defaults_are_consistent() -> None:
    s = Settings()
    assert s.done_ttl_seconds >= 10 * s.job_lifetime_bound
    assert 2 * s.heartbeat_interval <= s.visibility_timeout
