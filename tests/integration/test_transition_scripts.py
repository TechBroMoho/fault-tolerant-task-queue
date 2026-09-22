"""The Phase 2 Lua scripts in isolation, against real Redis.

These drive the scripts directly (no worker loop), so each ownership rule is pinned down
exactly: the owner's call works, a non-owner's call changes NOTHING and returns
LEASE_LOST, and a re-send after a lost reply is harmless (SPEC §4, ADR-006, ADR-024).
Consumers are plain names here ("a", "b"), as if they were two workers.
"""

import asyncio
import json
from typing import Any

import pytest
import redis.asyncio as aioredis

from ftq import dlq
from ftq.client import Client
from ftq.config import Settings
from ftq.keys import Keys
from ftq.metrics import read_counters
from ftq.models import Job
from ftq.reaper import Reaper
from ftq.scheduler import Scheduler
from ftq.transitions import Commit, DeadReason, Outcome, Transitions

from .helpers import add_entry, deliver, entries, fast, hash_of, pel_size, pending, wait_for

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


async def _setup(r: aioredis.Redis, settings: Settings, keys: Keys, job_type: str = "t") -> Job:
    """Enqueue one job and create the group. Returns the job as stored."""
    await Client(r, settings).enqueue(job_type, {"n": 1})
    await r.xgroup_create(keys.stream, settings.group, id="0", mkstream=True)
    return Job.from_fields((await entries(r, keys.stream))[0][1])


def _as(r: aioredis.Redis, settings: Settings, consumer: str) -> Transitions:
    return Transitions(r, settings, worker_id=consumer)


async def _steal(r: aioredis.Redis, settings: Settings, keys: Keys, entry: str, to: str) -> None:
    """What a reaper does after a lease expires: XCLAIM the entry to another consumer."""
    await r.xclaim(keys.stream, settings.group, to, 0, [entry])


# ---------------------------------------------------------------- heartbeat


async def test_heartbeat_resets_idle_without_bumping_delivery_count(
    r: aioredis.Redis, settings: Settings, keys: Keys
) -> None:
    await _setup(r, settings, keys)
    entry = await deliver(r, keys.stream, settings.group, "a")

    async def idle_enough() -> bool:
        return (await pending(r, keys.stream, settings.group))[0].idle_ms >= 150

    await wait_for(idle_enough)
    assert await _as(r, settings, "a").heartbeat(entry) is Outcome.OK

    [p] = await pending(r, keys.stream, settings.group)
    assert p.owner == "a"
    assert p.idle_ms < 100  # the lease clock restarted
    assert p.deliveries == 1  # JUSTID: a heartbeat is not a delivery
    assert (await read_counters(r, keys))["heartbeats"] == 1


async def test_two_workers_heartbeating_cannot_steal_the_lease_back_and_forth(
    r: aioredis.Redis, settings: Settings, keys: Keys
) -> None:
    """a held the job, stalled, and b reclaimed it. a resumes heartbeating. Without the
    ownership check, a's XCLAIM would take the entry back, b's would take it again, and
    the job would bounce between them forever."""
    await _setup(r, settings, keys)
    entry = await deliver(r, keys.stream, settings.group, "a")
    await _steal(r, settings, keys, entry, to="b")
    a, b = _as(r, settings, "a"), _as(r, settings, "b")

    for _ in range(5):
        assert await a.heartbeat(entry) is Outcome.LEASE_LOST
        assert await b.heartbeat(entry) is Outcome.OK
        assert [p.owner for p in await pending(r, keys.stream, settings.group)] == ["b"]

    counters = await read_counters(r, keys)
    assert (counters["heartbeats"], counters["lease_lost"]) == (5, 5)
    # Only the one real reclaim counted as a delivery; ten heartbeats added nothing.
    assert (await pending(r, keys.stream, settings.group))[0].deliveries == 2


async def test_heartbeat_after_commit_is_lease_lost(
    r: aioredis.Redis, settings: Settings, keys: Keys
) -> None:
    job = await _setup(r, settings, keys)
    entry = await deliver(r, keys.stream, settings.group, "a")
    a = _as(r, settings, "a")
    assert await a.commit(entry, job, "{}") is Commit.COMMITTED
    assert await a.heartbeat(entry) is Outcome.LEASE_LOST  # the entry is gone from the PEL


# ---------------------------------------------------------------- retry


async def test_retry_by_owner_moves_job_to_delayed_set(
    r: aioredis.Redis, settings: Settings, keys: Keys
) -> None:
    job = await _setup(r, settings, keys)
    entry = await deliver(r, keys.stream, settings.group, "a")
    now_ms = int((await r.time())[0]) * 1000

    assert await _as(r, settings, "a").retry(entry, job, delay_s=5.0) is Outcome.OK

    # Out of the stream and the PEL...
    assert await r.xlen(keys.stream) == 0
    assert await pel_size(r, keys.stream, settings.group) == 0
    # ...and parked in the delayed set as the same job, next attempt, due in ~5 s.
    delayed: Any = await r.zrange(keys.delayed, 0, -1, withscores=True)
    [(member, score)] = delayed
    flat = json.loads(member)
    retried = Job.from_fields(dict(zip(flat[::2], flat[1::2], strict=True)))
    assert retried == job.model_copy(update={"attempt": 1})
    assert now_ms + 4000 <= score <= now_ms + 7000
    assert (await read_counters(r, keys))["retried"] == 1


async def test_stale_worker_retry_is_lease_lost_and_changes_nothing(
    r: aioredis.Redis, settings: Settings, keys: Keys
) -> None:
    """The heart of the stale-worker case: a's lease expired and b took the job. a's
    handler then raises. a's retry must not schedule anything."""
    job = await _setup(r, settings, keys)
    entry = await deliver(r, keys.stream, settings.group, "a")
    await _steal(r, settings, keys, entry, to="b")

    assert await _as(r, settings, "a").retry(entry, job, delay_s=0) is Outcome.LEASE_LOST

    assert await r.zcard(keys.delayed) == 0  # nothing scheduled
    assert [p.owner for p in await pending(r, keys.stream, settings.group)] == ["b"]
    assert await r.xlen(keys.stream) == 1  # b's entry untouched
    counters = await read_counters(r, keys)
    assert (counters["retried"], counters["lease_lost"]) == (0, 1)


async def test_resent_retry_is_harmless(r: aioredis.Redis, settings: Settings, keys: Keys) -> None:
    job = await _setup(r, settings, keys)
    entry = await deliver(r, keys.stream, settings.group, "a")
    a = _as(r, settings, "a")
    assert await a.retry(entry, job, delay_s=0) is Outcome.OK
    assert await a.retry(entry, job, delay_s=0) is Outcome.LEASE_LOST  # already acked
    assert await r.zcard(keys.delayed) == 1


async def test_retry_of_a_job_that_already_succeeded_drops_the_copy(
    r: aioredis.Redis, settings: Settings, keys: Keys
) -> None:
    """Two entries for one job_id (a re-sent XADD). One copy succeeds; the other copy's
    handler raises. Its owner must not schedule a retry of a finished job."""
    job = await _setup(r, settings, keys)
    await add_entry(r, keys.stream, (await entries(r, keys.stream))[0][1])
    first = await deliver(r, keys.stream, settings.group, "a")
    second = await deliver(r, keys.stream, settings.group, "b")
    assert await _as(r, settings, "a").commit(first, job, "{}") is Commit.COMMITTED

    assert await _as(r, settings, "b").retry(second, job, delay_s=0) is Outcome.TERMINAL

    assert await r.zcard(keys.delayed) == 0
    assert await r.xlen(keys.stream) == 0
    assert await pel_size(r, keys.stream, settings.group) == 0
    assert (await read_counters(r, keys))["duplicates_suppressed"] == 1


# ---------------------------------------------------------------- dead (DLQ)


async def test_dead_by_owner_records_dlq_entry_and_terminal_state(
    r: aioredis.Redis, settings: Settings, keys: Keys
) -> None:
    job = await _setup(r, settings, keys)
    entry = await deliver(r, keys.stream, settings.group, "a")
    a = _as(r, settings, "a")

    outcome = await a.dead(entry, job.job_id, DeadReason.MAX_ATTEMPTS, "boom", attempts=3)
    assert outcome is Outcome.OK
    # A re-send after a lost reply adds no second DLQ entry.
    again = await a.dead(entry, job.job_id, DeadReason.MAX_ATTEMPTS, "boom", attempts=3)
    assert again is Outcome.LEASE_LOST

    [(dead_id, fields)] = await entries(r, keys.dead)
    # The original job fields survive verbatim (so it can be requeued)...
    assert Job.from_fields(fields) == job
    # ...plus why it died.
    assert fields["dlq_job_id"] == job.job_id
    assert fields["dlq_reason"] == "max_attempts"
    assert fields["dlq_error"] == "boom"
    assert (fields["dlq_attempts"], fields["dlq_deliveries"]) == ("3", "1")
    assert fields["dlq_source_entry_id"] == entry
    done = await hash_of(r, keys.done(job.job_id))
    assert (done["state"], done["dead_entry_id"], done["reason"]) == (
        "DEAD",
        dead_id,
        "max_attempts",
    )
    assert await r.ttl(keys.done(job.job_id)) == -1  # DEAD records don't expire
    assert await r.xlen(keys.stream) == 0
    assert await pel_size(r, keys.stream, settings.group) == 0
    assert (await read_counters(r, keys))["dead"] == 1


async def test_stale_worker_dead_is_lease_lost_and_changes_nothing(
    r: aioredis.Redis, settings: Settings, keys: Keys
) -> None:
    job = await _setup(r, settings, keys)
    entry = await deliver(r, keys.stream, settings.group, "a")
    await _steal(r, settings, keys, entry, to="b")

    outcome = await _as(r, settings, "a").dead(entry, job.job_id, DeadReason.MAX_ATTEMPTS, "x", 1)
    assert outcome is Outcome.LEASE_LOST
    assert await r.xlen(keys.dead) == 0
    assert not await r.exists(keys.done(job.job_id))
    assert [p.owner for p in await pending(r, keys.stream, settings.group)] == ["b"]


async def test_dead_never_replaces_succeeded(
    r: aioredis.Redis, settings: Settings, keys: Keys
) -> None:
    job = await _setup(r, settings, keys)
    await add_entry(r, keys.stream, (await entries(r, keys.stream))[0][1])
    first = await deliver(r, keys.stream, settings.group, "a")
    second = await deliver(r, keys.stream, settings.group, "b")
    assert await _as(r, settings, "a").commit(first, job, "{}") is Commit.COMMITTED

    b = _as(r, settings, "b")
    outcome = await b.dead(second, job.job_id, DeadReason.MAX_DELIVERIES, "x", 1)
    assert outcome is Outcome.TERMINAL

    assert (await hash_of(r, keys.done(job.job_id)))["state"] == "SUCCEEDED"
    assert await r.xlen(keys.dead) == 0
    assert await pel_size(r, keys.stream, settings.group) == 0


async def test_late_success_replaces_dead_and_removes_dlq_entry(
    r: aioredis.Redis, settings: Settings, keys: Keys
) -> None:
    """a stalls; b reclaims the job and, over max_deliveries, sends it to the DLQ. Then a
    finishes. The work was done, so SUCCEEDED replaces DEAD and the DLQ entry goes
    (SPEC §4 precedence, ADR-009)."""
    job = await _setup(r, settings, keys)
    entry = await deliver(r, keys.stream, settings.group, "a")
    await _steal(r, settings, keys, entry, to="b")
    b = _as(r, settings, "b")
    assert await b.dead(entry, job.job_id, DeadReason.MAX_DELIVERIES, "x", 1) is Outcome.OK
    assert await r.xlen(keys.dead) == 1

    assert await _as(r, settings, "a").commit(entry, job, '{"late":true}') is Commit.LATE_SUCCESS

    done = await hash_of(r, keys.done(job.job_id))
    assert done["state"] == "SUCCEEDED" and done["worker_id"] == "a"
    assert "dead_entry_id" not in done and "reason" not in done  # DEAD's fields are gone
    assert await r.xlen(keys.dead) == 0
    assert await r.xlen(keys.results) == 1
    counters = await read_counters(r, keys)
    assert (counters["late_successes"], counters["processed"], counters["dead"]) == (1, 1, 1)
    # A second late commit is an ordinary suppressed duplicate.
    assert await _as(r, settings, "a").commit(entry, job, "{}") is Commit.DUPLICATE


# ---------------------------------------------------------------- scheduler


async def test_scheduler_moves_only_due_jobs(
    r: aioredis.Redis, settings: Settings, keys: Keys
) -> None:
    now_ms = int((await r.time())[0]) * 1000
    due = json.dumps(["job_id", "due", "attempt", "1"])
    later = json.dumps(["job_id", "later", "attempt", "1"])
    await r.zadd(keys.delayed, {due: now_ms - 1000, later: now_ms + 60_000})

    assert await Scheduler(r, settings).move_due() == 1

    [(_id, fields)] = await entries(r, keys.stream)
    assert fields == {"job_id": "due", "attempt": "1"}
    assert await r.zrange(keys.delayed, 0, -1) == [later]
    assert (await read_counters(r, keys))["scheduled"] == 1


async def test_concurrent_schedulers_never_move_a_job_twice(
    r: aioredis.Redis, settings: Settings, keys: Keys
) -> None:
    """Every worker runs the scheduler. The move is atomic, so ten schedulers racing over
    200 due retries move each exactly once."""
    now_ms = int((await r.time())[0]) * 1000
    await r.zadd(keys.delayed, {json.dumps(["job_id", str(i)]): now_ms - 1 for i in range(200)})
    small = fast(settings, scheduler_batch=7)  # small batches, so the calls interleave

    async def drain() -> int:
        s, moved = Scheduler(r, small), 0
        while n := await s.move_due():
            moved += n
        return moved

    totals = await asyncio.gather(*(drain() for _ in range(10)))
    assert sum(totals) == 200
    moved_ids = sorted(fields["job_id"] for _id, fields in await entries(r, keys.stream))
    assert moved_ids == sorted(str(i) for i in range(200))


# ---------------------------------------------------------------- reaper


async def test_reclaim_takes_only_expired_entries_and_reports_deliveries(
    r: aioredis.Redis, settings: Settings, keys: Keys
) -> None:
    s = fast(settings)  # 0.5 s lease
    client = Client(r, s)
    await client.enqueue("t")
    await r.xgroup_create(keys.stream, s.group, id="0", mkstream=True)
    stale = await deliver(r, keys.stream, s.group, "crashed")

    async def expired() -> bool:
        return (await pending(r, keys.stream, s.group))[0].idle_ms >= 500

    await wait_for(expired)
    await client.enqueue("t")
    fresh = await deliver(r, keys.stream, s.group, "alive")  # idle ~0: NOT expired

    claimed, _more = await Reaper(r, s, "reaper").reclaim(count=10)

    assert [(c.entry_id, c.deliveries) for c in claimed] == [(stale, 2)]
    assert claimed[0].fields["type"] == "t"
    owners = {p.entry_id: p.owner for p in await pending(r, keys.stream, s.group)}
    assert owners == {stale: "reaper", fresh: "alive"}
    assert (await read_counters(r, keys))["reclaimed"] == 1


async def test_reclaim_respects_count(r: aioredis.Redis, settings: Settings, keys: Keys) -> None:
    s = fast(settings)
    client = Client(r, s)
    await r.xgroup_create(keys.stream, s.group, id="0", mkstream=True)
    for _ in range(5):
        await client.enqueue("t")
        await deliver(r, keys.stream, s.group, "crashed")

    async def all_expired() -> bool:
        return all(p.idle_ms >= 500 for p in await pending(r, keys.stream, s.group))

    await wait_for(all_expired)
    reaper = Reaper(r, s, "reaper")
    first, _ = await reaper.reclaim(count=2)
    assert len(first) == 2  # the caller's free slots bound the claim
    rest, _ = await reaper.reclaim(count=10)
    assert len(rest) == 3


# ---------------------------------------------------------------- requeue


async def test_requeue_puts_dead_job_back_with_attempt_zero(
    r: aioredis.Redis, settings: Settings, keys: Keys
) -> None:
    job = await _setup(r, settings, keys)
    entry = await deliver(r, keys.stream, settings.group, "a")
    a = _as(r, settings, "a")
    await a.dead(entry, job.job_id, DeadReason.MAX_ATTEMPTS, "x", attempts=5)

    assert await dlq.requeue(r, keys, job.job_id) is True
    assert await dlq.requeue(r, keys, job.job_id) is False  # re-send: no second copy

    [(_id, fields)] = await entries(r, keys.stream)
    assert not any(name.startswith("dlq_") for name in fields)
    assert Job.from_fields(fields) == job.model_copy(update={"attempt": 0})
    assert await r.xlen(keys.dead) == 0
    assert not await r.exists(keys.done(job.job_id))  # non-terminal again
    assert (await read_counters(r, keys))["requeued"] == 1


async def test_requeue_all_and_list(r: aioredis.Redis, settings: Settings, keys: Keys) -> None:
    client = Client(r, settings)
    await r.xgroup_create(keys.stream, settings.group, id="0", mkstream=True)
    job_ids = []
    for _ in range(3):
        job_ids.append(await client.enqueue("t"))
        entry = await deliver(r, keys.stream, settings.group, "a")
        await _as(r, settings, "a").dead(entry, job_ids[-1], DeadReason.UNKNOWN_TYPE, "?", 0)

    listed = await dlq.list_dead(r, keys)
    assert [d.job_id for d in listed] == job_ids
    assert {(d.type, d.reason, d.attempts, d.deliveries) for d in listed} == {
        ("t", "unknown_type", 0, 1)
    }
    assert await dlq.requeue_all(r, keys) == 3
    assert await dlq.list_dead(r, keys) == []
    stream_ids: Any = [f["job_id"] for _id, f in await entries(r, keys.stream)]
    assert stream_ids == job_ids
