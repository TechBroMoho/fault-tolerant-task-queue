"""Job schema and stream encoding (pure logic)."""

import uuid

import pytest
from pydantic import ValidationError

from ftq.models import Job, new_job_id


def test_job_id_is_uuid7_and_time_ordered() -> None:
    ids = [new_job_id() for _ in range(50)]
    assert all(uuid.UUID(i).version == 7 for i in ids)
    assert len(set(ids)) == 50
    # UUIDv7 leads with a millisecond timestamp, so ids sort by creation time
    # (the generator is monotonic within a millisecond too).
    assert ids == sorted(ids)


def test_fields_round_trip() -> None:
    job = Job(
        job_id=new_job_id(),
        type="send_email",
        payload={"to": "ada@example.com", "n": [1, 2], "nested": {"ok": True}},
        attempt=2,
        idempotency_key="order-7",
    )
    fields = job.to_fields()
    assert all(isinstance(v, str) for v in fields.values())  # stream fields are strings
    assert "enqueued_at_ms" not in fields  # stamped by enqueue.lua from Redis TIME
    parsed = Job.from_fields({**fields, "enqueued_at_ms": "1790000000000"})
    assert parsed == job.model_copy(update={"enqueued_at_ms": 1790000000000})


@pytest.mark.parametrize(
    "bad",
    [
        {"type": "t", "payload": "{}", "attempt": "0"},  # no job_id
        {"job_id": "x", "type": "t", "payload": "not json", "attempt": "0"},
        {"job_id": "x", "type": "t", "payload": "[1, 2]", "attempt": "0"},  # not an object
        {"job_id": "x", "type": "t", "payload": "{}", "attempt": "-1"},
        {"job_id": "x", "type": "t", "payload": "{}", "attempt": "one"},
    ],
)
def test_malformed_fields_rejected(bad: dict[str, str]) -> None:
    # The worker catches KeyError/ValueError (ValidationError is a ValueError).
    with pytest.raises((KeyError, ValueError, ValidationError)):
        Job.from_fields(bad)
