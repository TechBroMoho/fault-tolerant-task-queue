"""The worker's Redis connection pool covers everything it can have in flight at once
(ADR-044).

redis-py 8.1's asyncio pool defaults to 100 connections and raises MaxConnectionsError,
rather than waiting, when all of them are in use. The Phase 6 benchmark hit that at
concurrency 100. Here every in-flight job is made to hold two connections at the same
moment (its handler's ledger call and its heartbeat), plus the maintenance loop's: Redis
is paused with CLIENT PAUSE, so no command can finish and free its connection.
"""

import asyncio
import logging

import pytest
import redis.asyncio as aioredis

from ftq.client import Client, NewJob
from ftq.config import Settings, make_redis
from ftq.keys import Keys
from ftq.metrics import read_counters
from ftq.registry import JobContext, Registry

from .helpers import fast, running_worker, wait_for

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

N = 100  # concurrency: the value that hit the old limit in the benchmark
OLD_DEFAULT_POOL = 100  # redis-py 8.1's asyncio pool size when none is given
PAUSE_MS = 1000  # well under socket_timeout (5 s) and the lease below


async def _client_ids(redis_client: aioredis.Redis) -> set[int]:
    """Ids of the connections open on the server. Ids are never reused, so the ones not
    seen before are connections opened since, whatever else closes meanwhile."""
    return {int(c["id"]) for c in await redis_client.client_list()}


async def test_worker_pool_covers_every_connection_it_can_need_at_once(
    r: aioredis.Redis,
    redis_client: aioredis.Redis,
    settings: Settings,
    keys: Keys,
    caplog: pytest.LogCaptureFixture,
) -> None:
    # A 3 s lease, so the 1 s pause can't expire one; heartbeats still beat every 0.1 s.
    s = fast(settings, concurrency=N, visibility_timeout=3.0)
    registry = Registry()
    arrived = 0
    all_running = asyncio.Event()
    release = asyncio.Event()

    @registry.register("hold")
    async def hold(ctx: JobContext) -> str:
        nonlocal arrived
        arrived += 1
        if arrived == N:
            all_running.set()
        await release.wait()
        await ctx.ledger.apply(f"effect:{ctx.job.job_id}")
        return "ok"

    await Client(r, s).enqueue_many([NewJob("hold") for _ in range(N)])
    before = await _client_ids(redis_client)

    # The test's own polling uses `redis_client`: a poll through the worker's client
    # would take a connection from the pool under test.
    async def all_processed() -> bool:
        processed = await redis_client.hget(keys.stats, "processed")
        return int(processed or 0) == N

    # The worker's own client, built from the worker's settings as `ftq worker` does
    # (cli.run_worker). The fixture's `r` was built for the default concurrency.
    worker_redis = make_redis(s)
    with caplog.at_level(logging.WARNING, logger="ftq"):
        async with running_worker(worker_redis, s, registry):
            await asyncio.wait_for(all_running.wait(), timeout=10)
            # Nothing on any connection completes until the pause ends, so every command
            # the worker issues meanwhile holds its own connection.
            await redis_client.client_pause(PAUSE_MS, all=True)
            release.set()
            await wait_for(all_processed, within=15)
        # Counted before aclose() below closes the worker's pooled connections.
        opened = len(await _client_ids(redis_client) - before)
        await worker_redis.aclose()
        warnings = [rec.getMessage() for rec in caplog.records if rec.name.startswith("ftq")]

    # None of it failed: no MaxConnectionsError from a heartbeat, the fetch or
    # maintenance loop, or a handler (which would have cost its job an attempt).
    assert not warnings, f"{len(warnings)} warning(s), first: {warnings[:3]}"
    counters = await read_counters(r, keys)
    assert counters["processed"] == N
    assert counters["effects_applied"] == N
    assert counters["retried"] == 0
    # And the test wasn't vacuous: the pool really went past redis-py's default of 100
    # connections. (Pooled connections stay open until aclose(), so every one it opened
    # is counted.) The nominal peak is 2N + 1 (each job's ledger call and heartbeat, and
    # the maintenance loop), but a beat that lands just before the pause starts frees
    # its connection, so 199-201 are all seen.
    assert opened > OLD_DEFAULT_POOL, opened
