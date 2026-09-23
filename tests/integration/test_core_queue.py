"""Phase 1 acceptance: enqueue → processed → result stored; idempotent enqueue;
a forced redelivery of an already-done job is suppressed (SPEC §7 Phase 1)."""

import json

import pytest
import redis.asyncio as aioredis

from ftq.client import Client
from ftq.config import Settings
from ftq.handlers import registry as builtin_registry
from ftq.keys import Keys
from ftq.metrics import read_counters
from ftq.registry import JobContext, Registry

from .helpers import add_entry, entries, hash_of, pel_size, running_worker, wait_for

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


async def _state(r: aioredis.Redis, keys: Keys, job_id: str) -> str | None:
    return (await hash_of(r, keys.done(job_id))).get("state")


async def test_enqueue_process_result_stored(
    r: aioredis.Redis, settings: Settings, keys: Keys
) -> None:
    # Enqueue BEFORE any worker exists: the group is created at ID 0, so this job must
    # still be delivered (a group created at `$` would silently skip it).
    job_id = await Client(r, settings).enqueue("send_email", {"to": "ada@example.com"})
    assert await r.xlen(keys.stream) == 1

    async with running_worker(r, settings, builtin_registry):

        async def succeeded() -> bool:
            return await _state(r, keys, job_id) == "SUCCEEDED"

        await wait_for(succeeded)

    done = await hash_of(r, keys.done(job_id))
    assert done["state"] == "SUCCEEDED"
    assert json.loads(done["result"]) == {"to": "ada@example.com", "sent_now": True}
    assert done["worker_id"] == "test-worker"
    assert int(done["finished_at_ms"]) > 0

    # The entry left both the stream and the PEL (XACK + XDEL, ADR-016).
    assert await r.xlen(keys.stream) == 0
    assert await pel_size(r, keys.stream, settings.group) == 0

    # Exactly one result and one effect were recorded, in the append-only logs.
    results = await entries(r, keys.results)
    assert [fields["job_id"] for _id, fields in results] == [job_id]
    assert int(results[0][1]["finished_at_ms"]) >= int(results[0][1]["enqueued_at_ms"]) > 0
    effects = await entries(r, keys.effects)
    assert [fields["key"] for _id, fields in effects] == [f"send_email:{job_id}"]

    counters = await read_counters(r, keys)
    assert counters["processed"] == 1
    assert counters["duplicates_suppressed"] == 0
    assert counters["effects_applied"] == 1


async def test_idempotent_enqueue_returns_original_id(
    r: aioredis.Redis, settings: Settings, keys: Keys
) -> None:
    client = Client(r, settings)
    first = await client.enqueue("send_email", {"to": "a@example.com"}, idempotency_key="order-42")
    # Same key, even with a different payload: the original job_id, and no new entry.
    again = await client.enqueue("send_email", {"to": "b@example.com"}, idempotency_key="order-42")
    assert again == first
    assert await r.xlen(keys.stream) == 1

    # A different key (or none) is a different job.
    other = await client.enqueue("send_email", {"to": "a@example.com"}, idempotency_key="order-43")
    unkeyed = await client.enqueue("send_email", {"to": "a@example.com"})
    assert len({first, other, unkeyed}) == 3
    assert await r.xlen(keys.stream) == 3

    # The stored job carries its idempotency key.
    stored = await entries(r, keys.stream)
    assert stored[0][1]["idempotency_key"] == "order-42"


@pytest.mark.slow  # > 1 s: runs in `make test-all` and CI
async def test_idempotency_key_expires_after_ttl(r: aioredis.Redis, settings: Settings) -> None:
    short = settings.model_copy(update={"idempotency_ttl_seconds": 1})
    keys = Keys(short.queue)
    client = Client(r, short)
    first = await client.enqueue("send_email", idempotency_key="k")
    assert 0 < await r.ttl(keys.idem("k")) <= 1

    async def key_gone() -> bool:
        return not await r.exists(keys.idem("k"))

    await wait_for(key_gone, within=3)
    # Outside the dedup window, the same key creates a new job.
    assert await client.enqueue("send_email", idempotency_key="k") != first


async def test_forced_redelivery_of_done_job_is_suppressed(
    r: aioredis.Redis, settings: Settings, keys: Keys
) -> None:
    """Deliver the same job twice: the handler runs twice, but the effect and the
    result are each recorded once, and the second commit counts as a duplicate."""
    calls: list[str] = []
    registry = Registry()

    @registry.register("send_email")
    async def send_email(ctx: JobContext) -> dict[str, str]:
        calls.append(ctx.job.job_id)
        await ctx.ledger.apply(f"send_email:{ctx.job.job_id}", to="ada@example.com")
        return {"ok": "yes"}

    job_id = await Client(r, settings).enqueue("send_email")
    original = (await entries(r, keys.stream))[0][1]

    async with running_worker(r, settings, registry):

        async def committed() -> bool:
            return (await read_counters(r, keys))["processed"] == 1

        await wait_for(committed)

        # Force a redelivery: the identical job (same job_id) back on the stream, exactly
        # what a re-sent XADD after a lost reply produces (ADR-006). A reclaimed copy
        # (Phase 2) reaches commit.lua the same way.
        await add_entry(r, keys.stream, original)

        async def suppressed() -> bool:
            return (await read_counters(r, keys))["duplicates_suppressed"] == 1

        await wait_for(suppressed)

    assert calls == [job_id, job_id]  # at-least-once: the handler really ran twice
    counters = await read_counters(r, keys)
    assert counters["processed"] == 1
    assert counters["duplicates_suppressed"] == 1
    assert counters["effects_applied"] == 1  # ledger count stays 1
    assert counters["effects_suppressed"] == 1
    assert await r.xlen(keys.effects) == 1
    assert await r.xlen(keys.results) == 1
    # The duplicate's entry was acked and deleted too.
    assert await r.xlen(keys.stream) == 0
    assert await pel_size(r, keys.stream, settings.group) == 0


async def test_many_jobs_each_completed_once(
    r: aioredis.Redis, settings: Settings, keys: Keys
) -> None:
    """A burst larger than the in-flight cap: every job ends SUCCEEDED exactly once."""
    n = 300
    client = Client(r, settings)
    job_ids = [await client.enqueue("send_email", {"to": f"u{i}@example.com"}) for i in range(n)]

    async with running_worker(r, settings.model_copy(update={"concurrency": 7}), builtin_registry):

        async def all_done() -> bool:
            return (await read_counters(r, keys))["processed"] == n

        await wait_for(all_done, within=15)

    result_ids = [fields["job_id"] for _id, fields in await entries(r, keys.results)]
    assert sorted(result_ids) == sorted(job_ids)  # each exactly once
    effect_keys = [fields["key"] for _id, fields in await entries(r, keys.effects)]
    assert sorted(effect_keys) == sorted(f"send_email:{j}" for j in job_ids)
    for job_id in job_ids:
        assert await _state(r, keys, job_id) == "SUCCEEDED"
    assert await r.xlen(keys.stream) == 0
    assert await pel_size(r, keys.stream, settings.group) == 0
    assert (await read_counters(r, keys))["duplicates_suppressed"] == 0
