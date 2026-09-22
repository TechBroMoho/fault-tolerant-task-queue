"""The job schema and its stream encoding.

A stream entry is a flat map of strings, so a job is stored as one field per attribute
with the payload as a JSON string. Parsing goes through pydantic because stream data is
untrusted input: a malformed entry should fail loudly at the boundary, not deep inside a
handler.
"""

import json
from typing import Any

import uuid_utils
from pydantic import BaseModel, ConfigDict, Field


def new_job_id() -> str:
    """UUIDv7: time-ordered (sorts by creation, useful when reading logs) and globally
    unique without coordination. Generated on the client *before* XADD so a retried
    XADD yields two entries with the same job_id, which commit dedups (ADR-006).
    """
    return str(uuid_utils.uuid7())


class Job(BaseModel):
    model_config = ConfigDict(frozen=True)

    job_id: str
    type: str
    payload: dict[str, Any] = Field(default_factory=dict)
    # 0 on first run; Phase 2 retries re-enqueue with attempt + 1.
    attempt: int = Field(default=0, ge=0)
    # Producer-supplied enqueue dedup key, or "" if none.
    idempotency_key: str = ""
    # Set by the enqueue script from Redis TIME (one clock for all latency math, ADR-020).
    # 0 only before the job has been enqueued.
    enqueued_at_ms: int = 0

    def to_fields(self) -> dict[str, str]:
        """Stream fields, minus enqueued_at_ms, which the enqueue script stamps."""
        return {
            "job_id": self.job_id,
            "type": self.type,
            "payload": json.dumps(self.payload, separators=(",", ":")),
            "attempt": str(self.attempt),
            "idempotency_key": self.idempotency_key,
        }

    @classmethod
    def from_fields(cls, fields: dict[str, str]) -> "Job":
        return cls(
            job_id=fields["job_id"],
            type=fields["type"],
            payload=json.loads(fields["payload"]),
            attempt=int(fields["attempt"]),
            idempotency_key=fields.get("idempotency_key", ""),
            enqueued_at_ms=int(fields.get("enqueued_at_ms", "0")),
        )
