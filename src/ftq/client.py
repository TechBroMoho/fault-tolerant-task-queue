"""Producer API: `enqueue()` / `enqueue_many()` put jobs on the queue, with backpressure.

Backpressure (ADR-031): the enqueue script refuses a job when the queue's depth (stream
length + delayed retries) has reached `high_watermark`, and keeps refusing until the
depth falls below `low_watermark`. The check is inside the script, atomic with the
XADD, so the depth can't overshoot the high watermark however many producers there are.
What the producer does with a refusal is its mode:

- `reject`: raise `QueueFull` at once. The caller decides (drop, shed load, return 429).
- `block`: wait, re-asking every `block_poll_interval`, until the job is admitted or
  `block_timeout` passes (then raise `QueueFull`). This throttles a producer to the
  rate the workers drain the queue.

A job is accepted exactly when `enqueue()` returns (or when `enqueue_many()` reports its
id). A refused job wrote nothing: it is not in the queue and never will be.
"""

import asyncio
import random
from collections.abc import Sequence
from dataclasses import dataclass, field
from typing import Any, Literal

import redis.asyncio as aioredis

from ftq.config import Settings
from ftq.keys import Keys
from ftq.lua import register
from ftq.models import Job, new_job_id

_ADDED, _FULL = 1, -1  # enqueue.lua statuses (0 = duplicate)


class QueueFull(Exception):
    """The queue is at its high watermark (or hasn't drained below the low one yet).

    `accepted` is set by `enqueue_many`: one entry per input job, the job_id if that job
    was accepted, None if it was refused. `enqueue` leaves it empty.
    """

    def __init__(self, depth: int, accepted: list[str | None] | None = None) -> None:
        super().__init__(f"queue full (depth {depth})")
        self.depth = depth
        self.accepted = accepted or []


@dataclass(frozen=True, slots=True)
class NewJob:
    """One job for `enqueue_many`."""

    type: str
    payload: dict[str, Any] = field(default_factory=dict)
    idempotency_key: str | None = None


@dataclass
class ProducerCounters:
    """This client's own view, for load generators. (The queue-wide `rejected` and
    `blocked` counters in the stats hash are kept by the script; metrics.py.)"""

    accepted: int = 0  # new jobs added (idempotent repeats not included)
    duplicates: int = 0  # enqueues answered with an existing job_id
    rejected: int = 0  # jobs refused: QueueFull raised for them
    blocked: int = 0  # jobs that had to wait for room (block mode), accepted or not
    blocked_seconds: float = 0.0  # total time spent waiting for room


class Client:
    def __init__(
        self, redis: aioredis.Redis, settings: Settings, rng: random.Random | None = None
    ) -> None:
        self._redis = redis
        self._settings = settings
        self._keys = Keys(settings.queue)
        self._enqueue = register(redis, "enqueue")
        self._rng = rng or random.Random()  # poll jitter in block mode
        self.counters = ProducerCounters()

    async def enqueue(
        self,
        job_type: str,
        payload: dict[str, Any] | None = None,
        *,
        idempotency_key: str | None = None,
    ) -> str:
        """Add a job and return its job_id, or raise QueueFull (see the module docstring).

        Once this returns, the job is accepted: it will end in exactly one terminal
        state (SPEC §4). With `idempotency_key`, a repeat enqueue within
        `idempotency_ttl_seconds` adds nothing and returns the ORIGINAL job_id, even if
        the repeat's payload differs (the key is the producer's promise that it's the
        same job; we don't compare payloads), and even if the queue is full.
        """
        [job_id] = await self.enqueue_many([NewJob(job_type, payload or {}, idempotency_key)])
        return job_id

    async def enqueue_many(self, jobs: Sequence[NewJob]) -> list[str]:
        """Add many jobs in one pipelined round trip; return their job_ids in order.

        Each job is admitted on its own (a batch can straddle the watermark). In reject
        mode, if any job is refused, QueueFull is raised with `.accepted` saying which
        ones got in. In block mode, the refused ones are re-sent until admitted or
        `block_timeout` passes.
        """
        if not jobs:
            return []
        pending = {i: self._args(job) for i, job in enumerate(jobs)}
        accepted: list[str | None] = [None] * len(jobs)
        block = self._settings.backpressure_mode == "block"
        loop = asyncio.get_running_loop()
        started = loop.time()
        waited = False
        while True:
            count_as: Literal["rejected", "blocked", "none"] = (
                "none" if waited else ("blocked" if block else "rejected")
            )
            replies = await self._send(list(pending.values()), count_as)
            depth = 0
            for i, (job_id, status, reply_depth) in zip(list(pending), replies, strict=True):
                if status == _FULL:
                    depth = int(reply_depth)
                    continue
                accepted[i] = str(job_id)
                del pending[i]
                if status == _ADDED:
                    self.counters.accepted += 1
                else:
                    self.counters.duplicates += 1
            if not pending:
                break
            if not waited and block:
                self.counters.blocked += len(pending)
            waited = True
            deadline = started + self._settings.block_timeout
            if not block or loop.time() >= deadline:
                self.counters.rejected += len(pending)
                if block:
                    self.counters.blocked_seconds += loop.time() - started
                    # Reject mode counted these in the script; a block that gave up is a
                    # rejection too, so the queue-wide counter means "QueueFull raised".
                    await self._redis.hincrby(self._keys.stats, "rejected", len(pending))
                raise QueueFull(depth, accepted)
            # Jitter the poll so blocked producers don't all retry in the same instant.
            pause = self._settings.block_poll_interval * self._rng.uniform(0.5, 1.5)
            await asyncio.sleep(min(pause, max(0.0, deadline - loop.time())))
        if waited:
            self.counters.blocked_seconds += loop.time() - started
        return [job_id for job_id in accepted if job_id is not None]

    def _args(self, new: NewJob) -> tuple[list[str], list[str]]:
        """KEYS and ARGV for one job, minus the refusal counter (set per send)."""
        job = Job(
            job_id=new_job_id(),
            type=new.type,
            payload=new.payload,
            idempotency_key=new.idempotency_key or "",
        )
        k = self._keys
        keys = [k.stream, k.delayed, k.full, k.stats]
        if new.idempotency_key:
            keys.append(k.idem(new.idempotency_key))
        args = [
            job.job_id,
            str(self._settings.idempotency_ttl_seconds),
            str(self._settings.high_watermark),
            str(self._settings.low_watermark),
        ]
        for name, value in job.to_fields().items():
            args += [name, value]
        return keys, args

    async def _send(
        self, calls: list[tuple[list[str], list[str]]], count_as: str
    ) -> list[tuple[str, int, int]]:
        """One round trip: a single script call, or a pipeline of them."""

        def argv(args: list[str]) -> list[str]:
            return [*args[:4], count_as, *args[4:]]

        if len(calls) == 1:
            keys, args = calls[0]
            reply: Any = await self._enqueue(keys=keys, args=argv(args))
            return [(reply[0], int(reply[1]), int(reply[2]))]
        # Not a MULTI transaction: each script is atomic by itself, and a batch doesn't
        # need all-or-nothing semantics (each job is admitted on its own merits).
        pipe = self._redis.pipeline(transaction=False)
        for keys, args in calls:
            await self._enqueue(keys=keys, args=argv(args), client=pipe)
        replies: Any = await pipe.execute()
        return [(r[0], int(r[1]), int(r[2])) for r in replies]
