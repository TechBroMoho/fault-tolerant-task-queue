"""Handlers that misbehave on purpose, for the reliability tests and the chaos mix.

Each one that can succeed performs its effect through the ledger, keyed by job_id, so
the chaos verifier can count effects per job (invariant I2).
"""

import asyncio
import os
import time
from typing import Any

from ftq.models import Job
from ftq.registry import JobContext

# The timeout the hang handlers are registered with (handlers/__init__.py): short, so a
# chaos run sees many timeouts without spending minutes on each (ADR-030, ADR-036).
HANG_TIMEOUT = 2.0


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


def _hangs(job: Job) -> bool:
    """True on the attempts that should hang: attempt < `hang_attempts` (payload, default
    1). Deterministic like `flaky`: a job knows how many timeouts it will take, and one
    with hang_attempts >= max_attempts always hangs and must end in the DLQ."""
    return job.attempt < int(job.payload.get("hang_attempts", 1))


def _hang_seconds(job: Job) -> float:
    """How long a hung run lasts if nobody stops it (payload `hang_seconds`, default 30):
    far past HANG_TIMEOUT."""
    return float(job.payload.get("hang_seconds", 30.0))


async def hang(ctx: JobContext) -> dict[str, Any]:
    """Async: hangs (a long await) on the first `hang_attempts` attempts, then applies its
    effect. A timeout cancels the hung run at its await (ADR-030)."""
    if _hangs(ctx.job):
        await asyncio.sleep(_hang_seconds(ctx.job))
    applied = await ctx.ledger.apply(f"hang:{ctx.job.job_id}")
    return {"attempt": ctx.job.attempt, "applied_now": applied}


def hang_thread(job: Job) -> dict[str, Any]:
    """Thread pool: blocks its thread on the first `hang_attempts` attempts. A thread can't
    be stopped, so a timed-out run becomes an orphan that holds its slot until the sleep
    ends, and whatever it returns is discarded (ADR-030). No effect: sync handlers have
    no ledger (ADR-028)."""
    if _hangs(job):
        time.sleep(_hang_seconds(job))
    return {"attempt": job.attempt}


def hang_process(job: Job) -> dict[str, Any]:
    """Process pool: blocks its pool child on the first `hang_attempts` attempts. A
    timeout resets the pool (every child SIGKILLed), and the other jobs that were
    running in it restart at the same attempt (ADR-030). No effect, like hang_thread."""
    if _hangs(job):
        time.sleep(_hang_seconds(job))
    return {"attempt": job.attempt}
