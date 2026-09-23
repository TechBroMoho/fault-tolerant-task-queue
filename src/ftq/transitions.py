"""The state changes of one delivered stream entry, each one Lua script (ADR-017, ADR-024).

- commit: first-wins and idempotent. ANY holder may commit, and later commits for the
  same job_id become suppressed duplicates. No ownership check.
- heartbeat, retry, dead: ownership-checked. The script first asks the PEL whether the
  caller still owns the entry, and does nothing but return LEASE_LOST if it doesn't.
  `XACK` and `XCLAIM` don't check ownership themselves, so without this a stale worker
  could schedule a retry of a job another worker already finished, or two workers could
  steal a lease back and forth (SPEC §4).

Every script is safe to re-send after a lost reply (ADR-006); each one's header says why.
"""

import enum
from typing import Any

import redis.asyncio as aioredis

from ftq.config import Settings
from ftq.keys import Keys
from ftq.lua import register
from ftq.models import Job


class Outcome(enum.StrEnum):
    OK = "OK"
    LEASE_LOST = "LEASE_LOST"  # the caller no longer owns the entry; nothing changed
    TERMINAL = "TERMINAL"  # another copy of the job already finished; this entry was dropped


class Commit(enum.IntEnum):
    DUPLICATE = 0  # the job had already SUCCEEDED; this delivery was suppressed
    COMMITTED = 1
    LATE_SUCCESS = 2  # committed, replacing an earlier DEAD (ADR-009)


class DeadReason(enum.StrEnum):
    MAX_ATTEMPTS = "max_attempts"  # the handler kept raising (poison job)
    MAX_DELIVERIES = "max_deliveries"  # the job keeps crashing its worker
    MALFORMED = "malformed"  # the entry can't be parsed as a job
    UNKNOWN_TYPE = "unknown_type"  # no handler is registered for its type


class Transitions:
    def __init__(self, redis: aioredis.Redis, settings: Settings, worker_id: str) -> None:
        self._settings = settings
        self._keys = Keys(settings.queue)
        self._worker_id = worker_id
        self._commit = register(redis, "commit")
        self._heartbeat = register(redis, "heartbeat")
        self._retry = register(redis, "retry")
        self._dead = register(redis, "dead")

    async def commit(self, entry_id: str, job: Job, result_json: str) -> Commit:
        k = self._keys
        outcome: Any = await self._commit(
            keys=[k.stream, k.done(job.job_id), k.results, k.stats, k.dead],
            args=[
                self._settings.group,
                entry_id,
                job.job_id,
                result_json,
                str(self._settings.done_ttl_seconds),
                self._worker_id,
                str(job.enqueued_at_ms),
            ],
        )
        return Commit(int(outcome))

    async def heartbeat(self, entry_id: str) -> Outcome:
        """Reset the entry's idle time (the lease clock) if we still own it."""
        outcome: Any = await self._heartbeat(
            keys=[self._keys.stream, self._keys.stats],
            args=[self._settings.group, entry_id, self._worker_id],
        )
        return Outcome(outcome)

    async def retry(
        self, entry_id: str, job: Job, delay_s: float, *, timed_out: bool = False
    ) -> Outcome:
        """Schedule attempt `job.attempt + 1` to run after `delay_s`, if we own the entry.
        `timed_out` also counts the failure in the `timeouts` counter (ADR-030)."""
        k = self._keys
        outcome: Any = await self._retry(
            keys=[k.stream, k.delayed, k.done(job.job_id), k.stats],
            args=[
                self._settings.group,
                entry_id,
                self._worker_id,
                str(job.attempt + 1),
                str(int(delay_s * 1000)),
                "1" if timed_out else "0",
            ],
        )
        return Outcome(outcome)

    async def dead(
        self,
        entry_id: str,
        job_id: str,
        reason: DeadReason,
        error: str,
        attempts: int,
        *,
        timed_out: bool = False,
    ) -> Outcome:
        """Move the entry's job to the DLQ with terminal state DEAD, if we own the entry."""
        k = self._keys
        outcome: Any = await self._dead(
            keys=[k.stream, k.dead, k.done(job_id), k.stats],
            args=[
                self._settings.group,
                entry_id,
                self._worker_id,
                job_id,
                reason.value,
                error,
                str(attempts),
                "1" if timed_out else "0",
            ],
        )
        return Outcome(outcome)
