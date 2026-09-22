"""Handlers that misbehave on purpose, for the reliability tests and the chaos mix.

Each one that can succeed performs its effect through the ledger, keyed by job_id, so
the chaos verifier can count effects per job (invariant I2).
"""

import asyncio
import os
from typing import Any

from ftq.registry import JobContext


class InjectedFailure(Exception):
    """Raised by flaky and poison jobs: a stand-in for a handler bug or a downstream 5xx."""


async def flaky(ctx: JobContext) -> dict[str, Any]:
    """Fails deterministically while `attempt < fail_times` (payload, default 1), then
    succeeds. Deterministic rather than random, so a test (or a 1M-job chaos run) knows
    exactly how many retries each job needs and never exhausts max_attempts by bad luck.
    """
    fail_times = int(ctx.job.payload.get("fail_times", 1))
    if ctx.job.attempt < fail_times:
        raise InjectedFailure(f"flaky: attempt {ctx.job.attempt} < fail_times {fail_times}")
    applied = await ctx.ledger.apply(f"flaky:{ctx.job.job_id}")
    return {"attempt": ctx.job.attempt, "applied_now": applied}


async def poison(ctx: JobContext) -> None:
    """Always raises, so the job ends in the DLQ after max_attempts, with no effect."""
    raise InjectedFailure("poison: this job always fails")


async def crashy(ctx: JobContext) -> None:
    """Kills the whole worker process mid-job, like a segfault or the OOM killer.

    `os._exit` skips every cleanup: no commit, no retry, no graceful drain. The entry stays
    in the PEL, gets reclaimed, crashes the next worker, and so on, until its delivery
    count passes max_deliveries and a reaper sends it to the DLQ unrun.
    """
    os._exit(int(ctx.job.payload.get("exit_code", 70)))


async def slow(ctx: JobContext) -> dict[str, Any]:
    """Sleeps `seconds` (payload, default 1.0) WITHOUT heartbeats, then applies its effect.

    Registered with heartbeat=False, so a run longer than the lease is guaranteed to be
    reclaimed while the first holder is still running it. That gives real redeliveries
    and suppressed duplicates (ADR-007).
    """
    await asyncio.sleep(float(ctx.job.payload.get("seconds", 1.0)))
    applied = await ctx.ledger.apply(f"slow:{ctx.job.job_id}")
    return {"applied_now": applied}
