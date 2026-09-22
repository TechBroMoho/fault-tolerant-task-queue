"""Idle consumer cleanup never deletes a consumer that still owns pending entries.

`XGROUP DELCONSUMER` discards the consumer's pending entries: they leave the PEL, no
reaper can reclaim them, and the jobs are silently lost. Pruning must only ever delete
consumers that own nothing (ADR-029).
"""

import asyncio

import pytest
import redis.asyncio as aioredis

from ftq.client import Client
from ftq.config import Settings
from ftq.handlers import registry as builtin_registry
from ftq.keys import Keys
from ftq.metrics import read_counters
from ftq.reaper import Reaper

from .helpers import consumers, deliver, fast, hash_of, pending, running_worker, wait_for

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

# Prune consumers idle for 0.3 s. The lease stays long (30 s) so no reaper moves the
# crashed consumer's entry away during the test: it must survive on its own merits.
PRUNE_IDLE = 0.3


def _prune_settings(settings: Settings) -> Settings:
    return fast(
        settings,
        visibility_timeout=30.0,
        heartbeat_interval=10.0,
        consumer_prune_idle=PRUNE_IDLE,
        consumer_prune_interval=0.05,
    )


async def _idle_past_threshold(r: aioredis.Redis, keys: Keys, group: str, *names: str) -> None:
    async def idle() -> bool:
        info = await r.xinfo_consumers(keys.stream, group)
        idle_ms = {c["name"]: c["idle"] for c in info}
        return all(idle_ms[n] > PRUNE_IDLE * 1000 for n in names)

    await wait_for(idle)


async def test_consumer_with_pending_work_is_never_deleted(
    r: aioredis.Redis, settings: Settings, keys: Keys
) -> None:
    s = _prune_settings(settings)
    client = Client(r, s)
    job_id = await client.enqueue("send_email")
    await r.xgroup_create(keys.stream, s.group, id="0", mkstream=True)
    # "crashed" took the job and died without acking: it owns one pending entry.
    entry = await deliver(r, keys.stream, s.group, "crashed")
    # "restarted" is an old worker that exited cleanly: a consumer record owning nothing.
    await r.xgroup_createconsumer(keys.stream, s.group, "restarted")
    await _idle_past_threshold(r, keys, s.group, "crashed", "restarted")

    reaper = Reaper(r, s, "pruner")
    for _ in range(3):  # repeated passes don't wear it down either
        deleted = await reaper.prune_consumers()
        assert "crashed" not in deleted

    assert await consumers(r, keys.stream, s.group) == {"crashed": 1}
    # The job is still in the PEL, owned by "crashed", with its delivery count intact...
    [p] = await pending(r, keys.stream, s.group)
    assert (p.entry_id, p.owner, p.deliveries) == (entry, "crashed", 1)
    counters = await read_counters(r, keys)
    assert counters["consumers_pruned"] == 1  # "restarted" only

    # ...so a reaper can still reclaim it and a worker completes it: nothing was lost.
    # Once the reaper has moved the entry away, "crashed" owns nothing, and the live
    # worker's own maintenance loop prunes it.
    async with running_worker(
        r, fast(s, visibility_timeout=0.5, heartbeat_interval=0.1), builtin_registry
    ):

        async def succeeded() -> bool:
            return (await hash_of(r, keys.done(job_id))).get("state") == "SUCCEEDED"

        await wait_for(succeeded)

        async def crashed_pruned() -> bool:
            return "crashed" not in await consumers(r, keys.stream, s.group)

        await wait_for(crashed_pruned)
    assert await r.xlen(keys.effects) == 1
    assert (await read_counters(r, keys))["reclaimed"] == 1


async def test_prune_skips_busy_consumers_and_the_caller(
    r: aioredis.Redis, settings: Settings, keys: Keys
) -> None:
    s = _prune_settings(settings)
    await r.xgroup_create(keys.stream, s.group, id="0", mkstream=True)
    for name in ("pruner", "idle-empty"):
        await r.xgroup_createconsumer(keys.stream, s.group, name)
    await _idle_past_threshold(r, keys, s.group, "pruner", "idle-empty")
    await r.xgroup_createconsumer(keys.stream, s.group, "just-joined")  # idle ~0

    deleted = await Reaper(r, s, "pruner").prune_consumers()

    assert deleted == ["idle-empty"]
    assert set(await consumers(r, keys.stream, s.group)) == {"pruner", "just-joined"}


async def test_running_worker_prunes_only_empty_idle_consumers(
    r: aioredis.Redis, settings: Settings, keys: Keys
) -> None:
    """The same guarantee end to end: a live worker's maintenance loop cleans up after
    restarts, while a crashed consumer's unfinished job stays in the PEL."""
    s = _prune_settings(settings)
    client = Client(r, s)
    job_id = await client.enqueue("send_email")
    await r.xgroup_create(keys.stream, s.group, id="0", mkstream=True)
    entry = await deliver(r, keys.stream, s.group, "crashed")
    for i in range(3):
        await r.xgroup_createconsumer(keys.stream, s.group, f"old-{i}")
    await _idle_past_threshold(r, keys, s.group, "crashed", "old-0", "old-1", "old-2")

    async with running_worker(r, s, builtin_registry, worker_id="live"):

        async def pruned() -> bool:
            return (await read_counters(r, keys))["consumers_pruned"] >= 3

        await wait_for(pruned)
        # Watch the PEL across ~10 more prune passes. (A deliberate sleep: we are
        # observing that something does NOT happen over time, not waiting for an event.)
        for _ in range(10):
            await asyncio.sleep(s.consumer_prune_interval)
            [p] = await pending(r, keys.stream, s.group)
            assert (p.entry_id, p.owner) == (entry, "crashed")

    assert await consumers(r, keys.stream, s.group) == {"crashed": 1, "live": 0}
    assert not await r.exists(keys.done(job_id))  # not run yet: its lease hasn't expired
