"""Backpressure (ADR-031): enqueue refuses jobs at the high watermark, keeps refusing
until the depth falls below the low watermark (hysteresis), and the check is atomic
with the XADD, so concurrent producers can't overshoot.

Depth changes here come from a real worker, or from XDEL/ZADD on the queue's own keys
when a test needs an exact depth. The admission logic under test is never mocked.
"""

import asyncio
from typing import Any

import pytest
import redis.asyncio as aioredis

from ftq.client import Client, NewJob, QueueFull
from ftq.config import Settings
from ftq.handlers import registry as builtin_registry
from ftq.keys import Keys
from ftq.metrics import read_counters, snapshot

from .helpers import entries, fast, running_worker, wait_for, with_

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


async def _drop_entries(r: aioredis.Redis, keys: Keys, n: int) -> None:
    """Remove the n oldest stream entries, the way commits would (XDEL after ack)."""
    doomed = [entry_id for entry_id, _ in (await entries(r, keys.stream))[:n]]
    await r.xdel(keys.stream, *doomed)


async def test_reject_engages_at_the_high_watermark_and_releases_below_the_low(
    r: aioredis.Redis, settings: Settings, keys: Keys
) -> None:
    s = with_(settings, high_watermark=5, low_watermark=2)
    client = Client(r, s)
    for _ in range(5):
        await client.enqueue("send_email")  # depth 0..4 at admission: accepted
    with pytest.raises(QueueFull) as full:
        await client.enqueue("send_email")  # depth 5 = high: full
    assert full.value.depth == 5
    assert await r.xlen(keys.stream) == 5  # the refused job wrote nothing

    # Hysteresis: at depth 3 (below high, not below low) it is STILL full.
    await _drop_entries(r, keys, 2)
    with pytest.raises(QueueFull):
        await client.enqueue("send_email")
    # At depth 2 (= low, not below it) still full; at depth 1 it opens.
    await _drop_entries(r, keys, 1)
    with pytest.raises(QueueFull):
        await client.enqueue("send_email")
    await _drop_entries(r, keys, 1)
    await client.enqueue("send_email")
    # Open again, it accepts all the way back up to the high watermark.
    for _ in range(3):
        await client.enqueue("send_email")
    assert await r.xlen(keys.stream) == 5
    with pytest.raises(QueueFull):
        await client.enqueue("send_email")

    assert (await read_counters(r, keys))["rejected"] == 4
    assert client.counters.accepted == 9 and client.counters.rejected == 4


async def test_delayed_retries_count_toward_depth(
    r: aioredis.Redis, settings: Settings, keys: Keys
) -> None:
    """Retries waiting out their backoff will come back to the stream, so they are
    part of the depth (ADR-026). A queue whose work is all parked in the delayed set is
    not empty."""
    s = with_(settings, high_watermark=3, low_watermark=1)
    await r.zadd(keys.delayed, {"retry-a": 1, "retry-b": 2})
    client = Client(r, s)
    await client.enqueue("send_email")  # depth 2 -> 3
    with pytest.raises(QueueFull) as full:
        await client.enqueue("send_email")
    assert full.value.depth == 3


async def test_an_idempotent_repeat_is_answered_even_when_full(
    r: aioredis.Redis, settings: Settings, keys: Keys
) -> None:
    """A repeat adds nothing, so there is no reason to refuse it: the producer gets the
    original job_id. A NEW key is still refused, and refusing it leaves the key free."""
    s = with_(settings, high_watermark=1, low_watermark=0)
    client = Client(r, s)
    original = await client.enqueue("send_email", idempotency_key="order-1")
    with pytest.raises(QueueFull):
        await client.enqueue("send_email", idempotency_key="order-2")  # trips the full flag
    assert await r.exists(keys.full) == 1
    # The queue is now flagged full, and the repeat is still answered.
    assert await client.enqueue("send_email", idempotency_key="order-1") == original
    assert await r.exists(keys.idem("order-2")) == 0  # refused: nothing written
    assert await r.xlen(keys.stream) == 1
    assert client.counters.duplicates == 1


async def test_concurrent_producers_never_overshoot_the_high_watermark(
    r: aioredis.Redis, settings: Settings, keys: Keys
) -> None:
    """The admission check and the XADD are one atomic script, so however the enqueues
    of many producers interleave, exactly `high_watermark` jobs get in. A client-side
    check against a cached depth would let each producer overshoot."""
    s = with_(settings, high_watermark=50, low_watermark=10)
    clients = [Client(r, s) for _ in range(4)]
    accepted = rejected = 0

    async def produce(client: Client) -> None:
        nonlocal accepted, rejected
        for _ in range(40):
            try:
                await client.enqueue("send_email")
                accepted += 1
            except QueueFull:
                rejected += 1

    await asyncio.gather(*(produce(c) for c in clients))
    assert (accepted, rejected) == (50, 110)
    assert await r.xlen(keys.stream) == 50


async def test_enqueue_many_admits_each_job_and_reports_which_got_in(
    r: aioredis.Redis, settings: Settings, keys: Keys
) -> None:
    s = with_(settings, high_watermark=5, low_watermark=2)
    client = Client(r, s)
    with pytest.raises(QueueFull) as full:
        await client.enqueue_many([NewJob("send_email", {"n": i}) for i in range(8)])
    accepted = full.value.accepted
    assert len(accepted) == 8
    assert all(accepted[:5]) and accepted[5:] == [None, None, None]
    stream = await entries(r, keys.stream)
    assert [f["job_id"] for _id, f in stream] == accepted[:5]


async def test_block_mode_waits_for_room_then_times_out(
    r: aioredis.Redis, settings: Settings, keys: Keys
) -> None:
    s = with_(
        settings,
        high_watermark=2,
        low_watermark=1,
        backpressure_mode="block",
        block_timeout=0.3,
        block_poll_interval=0.02,
    )
    client = Client(r, s)
    await client.enqueue_many([NewJob("send_email"), NewJob("send_email")])
    loop = asyncio.get_running_loop()
    t0 = loop.time()
    with pytest.raises(QueueFull):
        await client.enqueue("send_email")
    assert 0.3 <= loop.time() - t0 < 1.0
    assert await r.xlen(keys.stream) == 2
    c = await read_counters(r, keys)
    # Counted once as blocked (not once per poll), and once as rejected when it gave up.
    assert (c["blocked"], c["rejected"]) == (1, 1)
    assert client.counters.blocked == 1 and client.counters.rejected == 1


async def test_block_mode_throttles_a_producer_to_the_workers_pace(
    r: aioredis.Redis, settings: Settings, keys: Keys
) -> None:
    """End to end: a blocking producer offers 60 jobs to a queue capped at 10 while a
    worker drains it at ~4 x 50 jobs/s. Every job is accepted (none rejected), the
    producer had to wait, the depth never went above the high watermark, and each job
    completed exactly once."""
    s = fast(
        settings,
        high_watermark=10,
        low_watermark=5,
        backpressure_mode="block",
        block_timeout=20.0,
        block_poll_interval=0.01,
        concurrency=4,
    )
    client = Client(r, s)
    max_depth = 0
    producing = True

    async def watch_depth() -> None:
        nonlocal max_depth
        while producing:
            max_depth = max(max_depth, (await snapshot(r, s))["depth"])
            await asyncio.sleep(0.005)

    async with running_worker(r, s, builtin_registry):
        watcher = asyncio.create_task(watch_depth())
        job_ids = [await client.enqueue("send_email", {"latency_ms": 20}) for _ in range(60)]
        producing = False
        await watcher

        async def all_done() -> bool:
            return (await read_counters(r, keys))["processed"] == 60

        await wait_for(all_done, within=20)
    assert 0 < max_depth <= 10
    assert client.counters.rejected == 0 and client.counters.blocked > 0
    results: list[Any] = [f["job_id"] for _id, f in await entries(r, keys.results)]
    assert sorted(results) == sorted(job_ids) and len(set(results)) == 60
    stats = await snapshot(r, s)
    assert (stats["depth"], stats["in_flight"], stats["full"]) == (0, 0, False)
