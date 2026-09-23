"""Per-job timeouts (ADR-030): a handler run that takes too long is a failed attempt.

What each kind of handler must do past its timeout:
- async: the handler is cancelled; the job goes down the retry path.
- process pool: the child running it is killed; the job goes down the retry path, and
  other jobs that shared the pool are restarted without losing an attempt.
- thread pool: the thread can't be stopped. Heartbeats stop, the job goes down the retry
  path, the orphaned thread keeps its slot until it returns, and whatever it returns is
  discarded: it never reaches a commit.
In every case a job that always hangs ends in the DLQ after max_attempts.
"""

import asyncio
import json
import logging
import os
import signal
import threading
import time
from pathlib import Path
from typing import Any

import pytest
import redis.asyncio as aioredis

from ftq.client import Client
from ftq.config import Settings
from ftq.keys import Keys
from ftq.metrics import read_counters
from ftq.models import Job
from ftq.registry import JobContext, Registry

from . import blocking_handlers
from .helpers import (
    entries,
    fast,
    hash_of,
    pel_size,
    running_worker,
    start_worker_process,
    wait_for,
)

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


async def _wait_state(
    r: aioredis.Redis, keys: Keys, job_id: str, state: str, within: float = 10
) -> dict[str, str]:
    async def reached() -> bool:
        return (await hash_of(r, keys.done(job_id))).get("state") == state

    await wait_for(reached, within=within)
    return await hash_of(r, keys.done(job_id))


async def _assert_drained(r: aioredis.Redis, keys: Keys, group: str) -> None:
    assert await r.xlen(keys.stream) == 0
    assert await pel_size(r, keys.stream, group) == 0
    assert await r.zcard(keys.delayed) == 0


# ---------------------------------------------------------------- async handlers


@pytest.mark.parametrize("where", ["per_type_override", "settings_default"])
async def test_async_handler_is_cancelled_and_the_job_retried(
    r: aioredis.Redis, settings: Settings, keys: Keys, where: str
) -> None:
    """Attempt 0 hangs; it is cancelled at the timeout and attempt 1 succeeds. The
    timeout comes from the handler's registration, or from Settings.job_timeout."""
    cancelled: list[int] = []
    registry = Registry()
    per_type = 0.3 if where == "per_type_override" else None

    @registry.register("hang_once", timeout=per_type)
    async def hang_once(ctx: JobContext) -> dict[str, Any]:
        if ctx.job.attempt == 0:
            try:
                await asyncio.sleep(3600)
            except asyncio.CancelledError:
                cancelled.append(ctx.job.attempt)
                raise
        applied = await ctx.ledger.apply(f"hang_once:{ctx.job.job_id}")
        return {"attempt": ctx.job.attempt, "applied_now": applied}

    s = fast(settings, job_timeout=300.0 if per_type else 0.3)
    job_id = await Client(r, s).enqueue("hang_once")
    async with running_worker(r, s, registry):
        done = await _wait_state(r, keys, job_id, "SUCCEEDED")
    assert json.loads(done["result"]) == {"attempt": 1, "applied_now": True}
    assert cancelled == [0]  # the hung run really was cancelled, not just ignored
    c = await read_counters(r, keys)
    assert (c["timeouts"], c["retried"], c["processed"], c["effects_applied"]) == (1, 1, 1, 1)
    assert len(await entries(r, keys.results)) == 1
    await _assert_drained(r, keys, s.group)


async def test_per_type_timeout_overrides_a_shorter_default(
    r: aioredis.Redis, settings: Settings, keys: Keys
) -> None:
    """A type registered with a longer timeout isn't cut off by a short default."""
    registry = Registry()

    @registry.register("patient", timeout=5.0)
    async def patient(ctx: JobContext) -> dict[str, Any]:
        await asyncio.sleep(0.5)
        return {"attempt": ctx.job.attempt}

    s = fast(settings, job_timeout=0.2)
    job_id = await Client(r, s).enqueue("patient")
    async with running_worker(r, s, registry):
        done = await _wait_state(r, keys, job_id, "SUCCEEDED")
    assert json.loads(done["result"]) == {"attempt": 0}
    assert (await read_counters(r, keys))["timeouts"] == 0


async def test_a_handler_that_swallows_its_cancellation_is_still_timed_out(
    r: aioredis.Redis, settings: Settings, keys: Keys, caplog: pytest.LogCaptureFixture
) -> None:
    """The worker never waits for a cancelled run to agree to stop. A handler that
    catches CancelledError and carries on is orphaned: the job is retried anyway, and
    what the orphan returns later is discarded (one result, from attempt 1)."""
    release = asyncio.Event()
    registry = Registry()

    @registry.register("stubborn", timeout=0.3)
    async def stubborn(ctx: JobContext) -> dict[str, Any]:
        if ctx.job.attempt == 0:
            try:
                await asyncio.sleep(3600)
            except asyncio.CancelledError:
                await release.wait()  # ignores the cancellation and keeps "working"
        return {"attempt": ctx.job.attempt}

    s = fast(settings)
    job_id = await Client(r, s).enqueue("stubborn")
    with caplog.at_level(logging.WARNING, logger="ftq.worker"):
        async with running_worker(r, s, registry):
            await _wait_state(r, keys, job_id, "SUCCEEDED")
            release.set()  # now the orphan finishes, after the retry already committed

            async def orphan_reported() -> bool:
                return any("orphaned async run finished" in m for m in caplog.messages)

            await wait_for(orphan_reported)
    done = await hash_of(r, keys.done(job_id))
    assert json.loads(done["result"]) == {"attempt": 1}
    assert len(await entries(r, keys.results)) == 1
    c = await read_counters(r, keys)
    assert (c["processed"], c["duplicates_suppressed"], c["timeouts"]) == (1, 0, 1)


async def test_a_job_that_always_hangs_ends_in_the_dlq(
    r: aioredis.Redis, settings: Settings, keys: Keys
) -> None:
    """Every attempt times out, and timeouts count as failed attempts."""
    registry = Registry()

    @registry.register("hangs", timeout=0.2)
    async def hangs(ctx: JobContext) -> None:
        await asyncio.sleep(3600)

    s = fast(settings, max_attempts=3)
    job_id = await Client(r, s).enqueue("hangs")
    async with running_worker(r, s, registry):
        await _wait_state(r, keys, job_id, "DEAD")
    [(_id, dead)] = await entries(r, keys.dead)
    assert (dead["dlq_reason"], dead["dlq_attempts"]) == ("max_attempts", "3")
    assert dead["dlq_error"].startswith("HandlerTimeout: run exceeded its 0.2s timeout")
    c = await read_counters(r, keys)
    assert (c["timeouts"], c["retried"], c["dead"], c["processed"]) == (3, 2, 1, 0)
    await _assert_drained(r, keys, s.group)


# ---------------------------------------------------------------- thread-pool handlers


def _blocking_first_attempt(started: threading.Event, release: threading.Event) -> Registry:
    registry = Registry()

    def block_first(job: Job) -> dict[str, Any]:
        if job.attempt == 0:
            started.set()
            release.wait(20)  # bounded, so a failing test can't leave a thread behind
            return {"attempt": 0, "late": True}
        return {"attempt": job.attempt}

    registry.register_sync("block_first", pool="thread", timeout=0.3)(block_first)
    return registry


async def test_orphaned_thread_returning_late_never_commits(
    r: aioredis.Redis, settings: Settings, keys: Keys, caplog: pytest.LogCaptureFixture
) -> None:
    """Attempt 0 blocks its thread past the timeout. The job is retried and attempt 1
    commits while the orphaned thread is still running. When the thread finally
    returns, its result is discarded without even reaching commit.lua:
    duplicates_suppressed stays 0, the results log has one entry, and the stored result
    is attempt 1's. (If a late commit did reach Redis, commit.lua's first-wins check
    would suppress it: test_stale_commit_after_the_reclaimer_scheduled_a_retry.)"""
    started, release = threading.Event(), threading.Event()
    registry = _blocking_first_attempt(started, release)
    s = fast(settings, concurrency=2)
    job_id = await Client(r, s).enqueue("block_first")
    try:
        with caplog.at_level(logging.WARNING, logger="ftq.worker"):
            async with running_worker(r, s, registry) as worker:
                done = await _wait_state(r, keys, job_id, "SUCCEEDED")
                assert json.loads(done["result"]) == {"attempt": 1}
                assert started.is_set() and worker.threads_still_running() == 1

                release.set()  # the orphan returns now, long after its timeout

                async def orphan_done() -> bool:
                    return worker.threads_still_running() == 0 and any(
                        "orphaned thread run finished" in m for m in caplog.messages
                    )

                await wait_for(orphan_done)
    finally:
        release.set()
    done = await hash_of(r, keys.done(job_id))
    assert json.loads(done["result"]) == {"attempt": 1}
    assert [f["job_id"] for _id, f in await entries(r, keys.results)] == [job_id]
    c = await read_counters(r, keys)
    assert (c["processed"], c["duplicates_suppressed"], c["timeouts"]) == (1, 0, 1)
    # Heartbeats for attempt 0 stopped at the timeout. One that kept going after the
    # retry acked the entry would have been refused and counted here.
    assert c["lease_lost"] == 0
    await _assert_drained(r, keys, s.group)


@pytest.mark.slow  # > 1 s: runs in `make test-all` and CI
async def test_orphaned_thread_keeps_its_slot_until_it_returns(
    r: aioredis.Redis, settings: Settings, keys: Keys
) -> None:
    """With concurrency 1, the orphaned thread holds the only slot, so the retry waits
    in the stream, undelivered, until the thread returns. Otherwise a worker full of
    hung threads would keep taking jobs it has no threads to run."""
    started, release = threading.Event(), threading.Event()
    registry = _blocking_first_attempt(started, release)
    s = fast(settings, concurrency=1)
    job_id = await Client(r, s).enqueue("block_first")
    try:
        async with running_worker(r, s, registry) as worker:

            async def retry_back_in_stream() -> bool:
                return (await read_counters(r, keys))["scheduled"] == 1

            await wait_for(retry_back_in_stream)
            # Watch for a while (20+ fetch-loop iterations at block_ms=100): the retry
            # stays undelivered because the only slot is taken.
            for _ in range(10):
                assert worker.threads_still_running() == 1
                assert await r.xlen(keys.stream) == 1
                assert await pel_size(r, keys.stream, s.group) == 0
                await asyncio.sleep(0.05)
            release.set()
            done = await _wait_state(r, keys, job_id, "SUCCEEDED")
    finally:
        release.set()
    assert json.loads(done["result"]) == {"attempt": 1}


@pytest.mark.slow
async def test_sigterm_exits_promptly_despite_a_hung_handler_thread(
    r: aioredis.Redis, settings: Settings, keys: Keys
) -> None:
    """A thread that never returns would keep the interpreter alive at exit forever
    (it joins pool threads). The CLI exits over it once the drain is done."""
    proc, _worker_id = await start_worker_process(
        settings,
        handlers="tests.integration.blocking_handlers:registry",
        env={"FTQ_MAX_ATTEMPTS": "1"},
    )
    try:
        job_id = await Client(r, settings).enqueue("hang_forever_in_thread")
        await _wait_state(r, keys, job_id, "DEAD")  # timed out after 0.5 s: 1 attempt
        t0 = time.monotonic()
        proc.send_signal(signal.SIGTERM)
        _out, err = await asyncio.wait_for(proc.communicate(), timeout=10)
    finally:
        if proc.returncode is None:
            proc.kill()
    assert proc.returncode == 0, err.decode()
    assert time.monotonic() - t0 < 5
    assert "1 handler thread(s) still running" in err.decode()
    [(_id, dead)] = await entries(r, keys.dead)
    assert dead["dlq_error"].startswith("HandlerTimeout")


# ---------------------------------------------------------------- process-pool handlers


def _runs(path: Path) -> list[int]:
    return [int(line) for line in path.read_text().split()] if path.exists() else []


def _pid_gone(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return True
    return False


@pytest.mark.slow
async def test_process_pool_timeout_kills_the_child_and_retries(
    r: aioredis.Redis, settings: Settings, keys: Keys, tmp_path: Path
) -> None:
    runs = tmp_path / "runs"
    s = fast(settings, process_pool_size=1)
    job_id = await Client(r, s).enqueue("hang_first_attempt_in_process", {"runs": str(runs)})
    async with running_worker(r, s, blocking_handlers.registry):
        done = await _wait_state(r, keys, job_id, "SUCCEEDED", within=20)
    first, second = _runs(runs)
    assert first != second  # attempt 1 ran in a fresh child

    async def killed() -> bool:
        return _pid_gone(first)

    await wait_for(killed)  # the hung child is really gone, not left sleeping
    assert json.loads(done["result"]) == {"attempt": 1, "pid": second}
    c = await read_counters(r, keys)
    assert (c["timeouts"], c["retried"], c["processed"]) == (1, 1, 1)


@pytest.mark.slow
async def test_a_pool_reset_restarts_bystanders_without_costing_them_an_attempt(
    r: aioredis.Redis, settings: Settings, keys: Keys, tmp_path: Path
) -> None:
    """Killing the timed-out job's child breaks the whole pool, so every child is
    killed. A job that was running next to it did nothing wrong: it restarts in the new
    pool at the same attempt, and only the hung job's failure is counted."""
    hung_runs, bystander_runs = tmp_path / "hung", tmp_path / "bystander"
    s = fast(settings, process_pool_size=2)
    client = Client(r, s)
    hung = await client.enqueue("hang_first_attempt_in_process", {"runs": str(hung_runs)})
    bystander = await client.enqueue(
        "spin_in_process", {"runs": str(bystander_runs), "seconds": 3.0}
    )
    async with running_worker(r, s, blocking_handlers.registry):
        by_done = await _wait_state(r, keys, bystander, "SUCCEEDED", within=30)
        await _wait_state(r, keys, hung, "SUCCEEDED", within=30)
    # The bystander was mid-run when the pool was reset (3 s of work vs the hung job's
    # 1.5 s timeout), so it ran twice, both times as attempt 0.
    assert len(_runs(bystander_runs)) == 2
    assert json.loads(by_done["result"])["attempt"] == 0
    c = await read_counters(r, keys)
    assert (c["timeouts"], c["retried"], c["processed"]) == (1, 1, 2)
    await _assert_drained(r, keys, s.group)


@pytest.mark.slow
async def test_waiting_for_a_pool_child_does_not_count_toward_the_timeout(
    r: aioredis.Redis, settings: Settings, keys: Keys, tmp_path: Path
) -> None:
    """Three 1 s jobs with a 1.5 s timeout share ONE pool child (concurrency 10). The
    third waits ~2 s for the child before it starts. If that wait counted, it would
    time out, and the pool reset would kill the job that was actually running."""
    s = fast(settings, process_pool_size=1)
    client = Client(r, s)
    job_ids = [
        await client.enqueue(
            "spin_briefly_in_process", {"runs": str(tmp_path / f"runs-{i}"), "seconds": 1.0}
        )
        for i in range(3)
    ]
    async with running_worker(r, s, blocking_handlers.registry):
        for job_id in job_ids:
            done = await _wait_state(r, keys, job_id, "SUCCEEDED", within=20)
            assert json.loads(done["result"])["attempt"] == 0
    c = await read_counters(r, keys)
    assert (c["timeouts"], c["retried"], c["processed"]) == (0, 0, 3)
    assert all(len(_runs(tmp_path / f"runs-{i}")) == 1 for i in range(3))  # none restarted
