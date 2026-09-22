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
