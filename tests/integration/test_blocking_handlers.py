"""Blocking and CPU-bound handlers must not starve heartbeats (ADR-028).

The lease is kept alive by a heartbeat task on the worker's event loop. A handler that
holds the loop for longer than the lease stops those heartbeats, the entry's idle time
passes the visibility timeout, and a reaper takes a perfectly healthy job away. That
doubles the work, and with enough of it pushes the job toward max_deliveries.

How it's measured: the test samples the entry's PEL record from its own process. Idle
time is kept by Redis, so a lapsed lease shows up as a large idle value whenever it's
read. The worker under test runs as a real subprocess where it matters, so the test's
own event loop (and so its sampling) can't be blocked by the handler.
"""

import asyncio
import signal

import pytest
import redis.asyncio as aioredis

from ftq.client import Client
from ftq.config import Settings
from ftq.handlers import registry as builtin_registry
from ftq.keys import Keys
from ftq.metrics import read_counters

from . import blocking_handlers
from .helpers import (
    entries,
    fast,
    hash_of,
    pending,
    running_worker,
    start_worker_process,
    wait_for,
    watching_lease,
)

pytestmark = [pytest.mark.integration, pytest.mark.asyncio, pytest.mark.slow]

LEASE = 1.0
LEASE_ENV = {
    "FTQ_VISIBILITY_TIMEOUT": str(LEASE),
    "FTQ_HEARTBEAT_INTERVAL": "0.2",
    "FTQ_REAP_INTERVAL": "0.05",
}
# ~2.6 s of SHA-256 on an M-series laptop (0.22 s per million rounds), so well over two
# leases. The test checks the actual run time, so a faster machine can't make it vacuous.
CPU_ROUNDS = 12_000_000


async def _owned_by(r: aioredis.Redis, keys: Keys, group: str, worker_id: str) -> None:
    async def owned() -> bool:
        return [p.owner for p in await pending(r, keys.stream, group)] == [worker_id]

    await wait_for(owned)


async def _stop(proc: asyncio.subprocess.Process) -> str:
    proc.send_signal(signal.SIGTERM)
    _out, err = await asyncio.wait_for(proc.communicate(), timeout=15)
    assert proc.returncode == 0, err.decode()
    return err.decode()


async def test_long_cpu_task_in_process_pool_keeps_its_lease(
    r: aioredis.Redis, settings: Settings, keys: Keys
) -> None:
    """A cpu_task running for over two leases, with a second worker's reaper armed the
    whole time. The job must never be reclaimed: its heartbeats keep running because the
    hashing happens in a pool process, not on the event loop."""
    proc, worker_a = await start_worker_process(settings, **LEASE_ENV)
    try:
        job_id = await Client(r, settings).enqueue("cpu_task", {"rounds": CPU_ROUNDS})
        await _owned_by(r, keys, settings.group, worker_a)

        # Worker B: its reaper would claim the entry the moment it's idle for LEASE.
        b_settings = fast(settings, visibility_timeout=LEASE, heartbeat_interval=0.2)
        async with (
            running_worker(r, b_settings, builtin_registry, worker_id="B"),
            watching_lease(r, keys.stream, settings.group) as watch,
        ):

            async def done() -> bool:
                return (await hash_of(r, keys.done(job_id))).get("state") == "SUCCEEDED"

            await wait_for(done, within=60)
    finally:
        stderr = await _stop(proc)

    result = await hash_of(r, keys.done(job_id))
    [(_id, entry)] = await entries(r, keys.results)
    run_ms = int(entry["finished_at_ms"]) - int(entry["enqueued_at_ms"])
    assert run_ms >= 2 * LEASE * 1000, f"job ran {run_ms} ms; raise CPU_ROUNDS"

    assert result["worker_id"] == worker_a  # A finished it; B never took it
    assert watch.owners == {worker_a}
    assert watch.deliveries == {1}
    assert watch.max_idle_ms < LEASE * 1000, watch
    assert watch.samples >= 20, watch
    counters = await read_counters(r, keys)
    assert counters["reclaimed"] == 0
    assert counters["duplicates_suppressed"] == 0
    assert counters["lease_lost"] == 0
    assert counters["heartbeats"] >= int(run_ms / 200) - 3, counters  # ~every 0.2 s
    assert "lost the lease" not in stderr


async def test_control_cpu_work_on_the_event_loop_does_lose_its_lease(
    r: aioredis.Redis, settings: Settings, keys: Keys
) -> None:
    """The same kind of work written the wrong way, as an async handler that never
    awaits. Its heartbeats can't run, so the lease lapses. This shows the measurement
    above can see starvation: if the pool version were starving too, it would fail."""
    proc, worker_a = await start_worker_process(
        settings, handlers="tests.integration.blocking_handlers:registry", **LEASE_ENV
    )
    try:
        job_id = await Client(r, settings).enqueue("hog_on_loop", {"seconds": 2.5 * LEASE})
        await _owned_by(r, keys, settings.group, worker_a)
        async with watching_lease(r, keys.stream, settings.group) as watch:

            async def done() -> bool:
                return (await hash_of(r, keys.done(job_id))).get("state") == "SUCCEEDED"

            await wait_for(done, within=30)
    finally:
        await _stop(proc)

    # The idle time passed the lease: any reaper would have reclaimed this healthy job.
    assert watch.max_idle_ms >= LEASE * 1000, watch


async def test_blocking_io_in_thread_pool_keeps_its_lease(
    r: aioredis.Redis, settings: Settings, keys: Keys
) -> None:
    """A synchronous blocking call (time.sleep standing in for a sync SDK) runs on a pool
    thread. The loop stays free, so heartbeats continue and a second worker's reaper
    never takes the job."""
    s = fast(settings)  # 0.5 s lease, heartbeat every 0.1 s
    job_id = await Client(r, s).enqueue("blocking_io", {"seconds": 3 * s.visibility_timeout})
    async with running_worker(r, s, blocking_handlers.registry, worker_id="A"):
        await _owned_by(r, keys, s.group, "A")
        async with (
            running_worker(r, s, blocking_handlers.registry, worker_id="B"),
            watching_lease(r, keys.stream, s.group) as watch,
        ):

            async def done() -> bool:
                return (await hash_of(r, keys.done(job_id))).get("state") == "SUCCEEDED"

            await wait_for(done)

    assert (await hash_of(r, keys.done(job_id)))["worker_id"] == "A"
    assert watch.owners == {"A"} and watch.deliveries == {1}
    assert watch.max_idle_ms < s.visibility_timeout * 1000, watch
    # This test's loop is the worker's loop. Many samples during the 1.5 s job show the
    # loop was never blocked; a blocked loop would also have frozen the sampler.
    assert watch.samples >= 30, watch
    assert (await read_counters(r, keys))["reclaimed"] == 0
