"""The dead-letter queue: inspect and requeue jobs whose terminal state is DEAD.

The DLQ is a stream (`Keys.dead`). Each entry is the job's original fields plus dlq_*
metadata: reason, last error, attempts, deliveries, time, worker (dead.lua, ADR-027). The
DLQ holds exactly the DEAD jobs. A late success deletes its job's entry (ADR-009), and a
requeue moves it back to the stream.
"""

from dataclasses import dataclass
from typing import Any

import redis.asyncio as aioredis

from ftq.keys import Keys
from ftq.lua import register

_PAGE = 100


@dataclass(frozen=True, slots=True)
class DeadJob:
    dlq_entry_id: str
    job_id: str
    type: str
    reason: str
    error: str
    attempts: int
    deliveries: int
    dead_at_ms: int

    @classmethod
    def from_entry(cls, entry_id: str, fields: dict[str, str]) -> "DeadJob":
        return cls(
            dlq_entry_id=entry_id,
            job_id=fields["dlq_job_id"],
            type=fields.get("type", ""),  # absent if the entry was malformed
            reason=fields["dlq_reason"],
            error=fields["dlq_error"],
            attempts=int(fields["dlq_attempts"]),
            deliveries=int(fields["dlq_deliveries"]),
            dead_at_ms=int(fields["dlq_dead_at_ms"]),
        )


async def list_dead(redis: aioredis.Redis, keys: Keys, limit: int = 100) -> list[DeadJob]:
    """The oldest `limit` DLQ entries."""
    reply: Any = await redis.xrange(keys.dead, count=limit)
    return [DeadJob.from_entry(eid, fields) for eid, fields in reply]


async def requeue(redis: aioredis.Redis, keys: Keys, job_id: str) -> bool:
    """Put one DEAD job back on the stream with attempt 0. False if it isn't DEAD.

    The job keeps its job_id, so the ledger still suppresses effects that already
    happened. Requeueing can't repeat an email that was actually sent.
    """
    script = register(redis, "requeue")
    requeued: Any = await script(
        keys=[keys.dead, keys.stream, keys.done(job_id), keys.stats], args=[job_id]
    )
    return int(requeued) == 1


async def requeue_all(redis: aioredis.Redis, keys: Keys) -> int:
    """Requeue every DLQ entry that is DEAD; return how many were requeued."""
    total = 0
    start = "-"
    while True:
        # Exclusive start after the last page, so an entry that couldn't be requeued
        # (and so is still there) isn't read again forever.
        page: Any = await redis.xrange(keys.dead, min=start, count=_PAGE)
        if not page:
            return total
        for _eid, fields in page:
            total += await requeue(redis, keys, fields["dlq_job_id"])
        start = f"({page[-1][0]}"
