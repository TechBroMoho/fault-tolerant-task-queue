"""commit.lua in isolation: first-wins, idempotent on re-send, owner-agnostic.

These drive the script directly (no worker) to pin down cases the worker loop can't
easily force: a commit re-sent after a lost reply, and a second holder committing an
already-done job.
"""

from typing import Any

import pytest
import redis.asyncio as aioredis
from redis.commands.core import AsyncScript

from ftq.client import Client
from ftq.config import Settings
from ftq.keys import Keys
from ftq.lua import register
from ftq.metrics import read_counters
from ftq.models import Job

from .helpers import add_entry, entries, hash_of, pel_size

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


async def _deliver(
    r: aioredis.Redis, settings: Settings, keys: Keys, consumer: str
) -> tuple[str, Job]:
    """XREADGROUP one entry as `consumer`, leaving it in that consumer's PEL."""
    reply: Any = await r.xreadgroup(settings.group, consumer, {keys.stream: ">"}, count=1)
    entry_id, fields = reply[0][1][0]
    return entry_id, Job.from_fields(fields)


async def _commit(
    script: AsyncScript,
    keys: Keys,
    settings: Settings,
    entry_id: str,
    job: Job,
    worker_id: str,
    ttl: int = 0,
) -> int:
    outcome = await script(
        keys=[keys.stream, keys.done(job.job_id), keys.results, keys.stats],
        args=[settings.group, entry_id, job.job_id, '{"n":1}', ttl, worker_id, job.enqueued_at_ms],
    )
    return int(outcome)


@pytest.fixture
def commit(r: aioredis.Redis) -> AsyncScript:
    return register(r, "commit")


async def _setup_group(r: aioredis.Redis, settings: Settings, keys: Keys) -> None:
    await r.xgroup_create(keys.stream, settings.group, id="0", mkstream=True)


async def test_resent_commit_is_suppressed(
    r: aioredis.Redis, settings: Settings, keys: Keys, commit: AsyncScript
) -> None:
    """The same commit twice (a re-send after a lost reply) records one result."""
    await Client(r, settings).enqueue("send_email")
    await _setup_group(r, settings, keys)
    entry_id, job = await _deliver(r, settings, keys, "a")

    assert await _commit(commit, keys, settings, entry_id, job, "a") == 1
    assert await _commit(commit, keys, settings, entry_id, job, "a") == 0

    assert await r.xlen(keys.results) == 1
    assert await r.xlen(keys.stream) == 0
    counters = await read_counters(r, keys)
    assert (counters["processed"], counters["duplicates_suppressed"]) == (1, 1)


async def test_second_holder_commit_is_suppressed_and_cleans_up(
    r: aioredis.Redis, settings: Settings, keys: Keys, commit: AsyncScript
) -> None:
    """Two stream entries for one job_id, held by two consumers. Whoever commits first
    wins; the other's commit is a duplicate that still removes ITS entry from the
    stream and the PEL, so nothing is left behind to be redelivered again."""
    await Client(r, settings).enqueue("send_email")
    original = (await entries(r, keys.stream))[0][1]
    await add_entry(r, keys.stream, original)  # duplicate entry, same job_id
    await _setup_group(r, settings, keys)
    entry_a, job_a = await _deliver(r, settings, keys, "a")
    entry_b, job_b = await _deliver(r, settings, keys, "b")
    assert job_a.job_id == job_b.job_id and entry_a != entry_b

    assert await _commit(commit, keys, settings, entry_b, job_b, "b") == 1
    assert await _commit(commit, keys, settings, entry_a, job_a, "a") == 0

    done = await hash_of(r, keys.done(job_a.job_id))
    assert done["worker_id"] == "b"  # first wins; the late commit changed nothing
    assert await r.xlen(keys.results) == 1
    assert await r.xlen(keys.stream) == 0
    assert await pel_size(r, keys.stream, settings.group) == 0


async def test_done_ttl_zero_means_no_expiry_and_positive_sets_it(
    r: aioredis.Redis, settings: Settings, keys: Keys, commit: AsyncScript
) -> None:
    client = Client(r, settings)
    await client.enqueue("send_email")
    await client.enqueue("send_email")
    await _setup_group(r, settings, keys)

    entry, job = await _deliver(r, settings, keys, "a")
    await _commit(commit, keys, settings, entry, job, "a", ttl=0)
    assert await r.ttl(keys.done(job.job_id)) == -1  # exists, never expires

    entry, job = await _deliver(r, settings, keys, "a")
    await _commit(commit, keys, settings, entry, job, "a", ttl=3600)
    assert 3590 < await r.ttl(keys.done(job.job_id)) <= 3600
