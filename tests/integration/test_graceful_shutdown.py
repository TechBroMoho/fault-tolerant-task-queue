"""SIGTERM drains in-flight work (SPEC §7 Phase 1).

These run a real `python -m ftq worker` subprocess and send it a real SIGTERM, because
signal handling is exactly the kind of thing an in-process test can fake by accident.
The worker's log lines on stderr are the synchronization points (no bare sleeps).
"""

import asyncio
import signal

import pytest
import redis.asyncio as aioredis

from ftq.client import Client
from ftq.config import Settings
from ftq.keys import Keys
from ftq.metrics import read_counters

from .helpers import hash_of, pel_size, read_until, start_worker_process, wait_for

pytestmark = [pytest.mark.integration, pytest.mark.asyncio, pytest.mark.slow]


async def _start_worker(settings: Settings, grace: float) -> asyncio.subprocess.Process:
    proc, _worker_id = await start_worker_process(settings, env={"FTQ_SHUTDOWN_GRACE": str(grace)})
    return proc


async def test_sigterm_lets_in_flight_job_finish_and_stops_fetching(
    r: aioredis.Redis, settings: Settings, keys: Keys
) -> None:
    proc = await _start_worker(settings, grace=10)
    client = Client(r, settings)
    slow = await client.enqueue("send_email", {"latency_ms": 1500})

    async def in_flight() -> bool:
        return await pel_size(r, keys.stream, settings.group) == 1

    await wait_for(in_flight)
    proc.send_signal(signal.SIGTERM)
    await read_until(proc, "stopped fetching; 1 job(s) in flight")

    # The fetch loop has exited: a job enqueued now must NOT be picked up.
    late = await client.enqueue("send_email")

    _stdout, stderr_tail = await asyncio.wait_for(proc.communicate(), timeout=10)
    assert proc.returncode == 0, stderr_tail.decode()
    assert "abandoning" not in stderr_tail.decode()

    # The in-flight job finished and committed during the drain...
    assert (await hash_of(r, keys.done(slow)))["state"] == "SUCCEEDED"
    assert await r.xlen(keys.effects) == 1
    # ...and the late job is still waiting, undelivered, for the next worker.
    assert not await r.exists(keys.done(late))
    assert await r.xlen(keys.stream) == 1
    assert await pel_size(r, keys.stream, settings.group) == 0
    assert (await read_counters(r, keys))["processed"] == 1


async def test_sigterm_past_grace_abandons_job_to_pel(
    r: aioredis.Redis, settings: Settings, keys: Keys
) -> None:
    """A job that outlives the grace period is abandoned, not lost: it stays in the PEL
    (uncommitted, no effect applied) for the Phase 2 reaper to reclaim."""
    proc = await _start_worker(settings, grace=0.5)
    job_id = await Client(r, settings).enqueue("send_email", {"latency_ms": 30_000})

    async def in_flight() -> bool:
        return await pel_size(r, keys.stream, settings.group) == 1

    await wait_for(in_flight)
    loop = asyncio.get_running_loop()
    signalled_at = loop.time()
    proc.send_signal(signal.SIGTERM)
    _stdout, stderr_tail = await asyncio.wait_for(proc.communicate(), timeout=10)
    elapsed = loop.time() - signalled_at

    assert proc.returncode == 0, stderr_tail.decode()
    assert "abandoning 1 in-flight job(s)" in stderr_tail.decode()
    assert elapsed < 5  # bounded by the grace period, not the 30 s job
    assert not await r.exists(keys.done(job_id))
    assert await r.xlen(keys.effects) == 0
    assert await r.xlen(keys.stream) == 1
    assert await pel_size(r, keys.stream, settings.group) == 1


async def test_sigterm_past_grace_terminates_process_pool_job(
    r: aioredis.Redis, settings: Settings, keys: Keys
) -> None:
    """A CPU-bound job in the process pool can't be cancelled like a coroutine, and the
    interpreter waits for pool children at exit. Past the grace period the worker must
    kill them, or shutdown wouldn't be bounded by the grace period at all (ADR-028)."""
    proc = await _start_worker(settings, grace=0.5)
    job_id = await Client(r, settings).enqueue("cpu_task", {"rounds": 500_000_000})  # ~minutes

    async def in_flight() -> bool:
        return await pel_size(r, keys.stream, settings.group) == 1

    await wait_for(in_flight)
    loop = asyncio.get_running_loop()
    signalled_at = loop.time()
    proc.send_signal(signal.SIGTERM)
    _stdout, stderr_tail = await asyncio.wait_for(proc.communicate(), timeout=10)
    elapsed = loop.time() - signalled_at

    assert proc.returncode == 0, stderr_tail.decode()
    assert "abandoning 1 in-flight job(s)" in stderr_tail.decode()
    assert elapsed < 5  # the grace period, not the job, bounded the shutdown
    assert not await r.exists(keys.done(job_id))
    assert await pel_size(r, keys.stream, settings.group) == 1  # left for a reaper
