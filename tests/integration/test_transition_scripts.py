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

from .helpers import (
    add_entry,
    deliver,
    entries,
    fast,
    hash_of,
    pel_size,
    pending,
    wait_for,
    with_,
)

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

    # Bounded: if a move weren't atomic (or didn't remove its member), the schedulers
    # would keep re-moving jobs forever; fail clearly instead of hanging until timeout.
    totals = await asyncio.wait_for(asyncio.gather(*(drain() for _ in range(10))), timeout=10)
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


async def _expired_entries(
    r: aioredis.Redis, s: Settings, keys: Keys, deliveries: list[int]
) -> list[str]:
    """One expired PEL entry per item, owned by "crashed", with that delivery count
    (XCLAIM's IDLE and RETRYCOUNT options set both directly)."""
    client = Client(r, s)
    await r.xgroup_create(keys.stream, s.group, id="0", mkstream=True)
    ids = []
    for n in deliveries:
        await client.enqueue("t")
        entry = await deliver(r, keys.stream, s.group, "crashed")
        await r.xclaim(
            keys.stream, s.group, "crashed", 0, [entry], idle=60_000, retrycount=n, justid=True
        )
        ids.append(entry)
    return ids


async def test_reclaim_takes_at_most_the_suspects_the_caller_has_room_for(
    r: aioredis.Redis, settings: Settings, keys: Keys
) -> None:
    """ADR-035: two entries that become suspects when claimed (delivery 3 >= the default
    threshold 3), and one that doesn't (delivery 2). With one suspect slot, the reaper
    takes the non-suspect and ONE suspect. The other suspect is put back exactly as it
    was: same delivery count, still expired, so another worker takes it at once."""
    s = fast(settings)
    first, second, plain = await _expired_entries(r, s, keys, [2, 2, 1])

    claimed, _more = await Reaper(r, s, "a").reclaim(count=10, suspect_slots=1)

    assert [(c.entry_id, c.deliveries) for c in claimed] == [(first, 3), (plain, 2)]
    assert [c.is_suspect(s) for c in claimed] == [True, False]
    [left] = [p for p in await pending(r, keys.stream, s.group) if p.entry_id == second]
    assert left.deliveries == 2  # unchanged: the put-back undid the claim's increment
    assert left.idle_ms >= 500  # still expired
    # A second worker with a free suspect slot takes it on its next pass.
    again, _more = await Reaper(r, s, "b").reclaim(count=10, suspect_slots=1)
    assert [(c.entry_id, c.deliveries) for c in again] == [(second, 3)]
    counters = await read_counters(r, keys)
    assert counters["reclaimed"] == 3  # put-backs aren't reclaims
    assert await hash_of(r, keys.reclaims) == {"3": "2", "2": "1"}


async def test_reclaim_with_no_suspect_slot_still_takes_non_suspects(
    r: aioredis.Redis, settings: Settings, keys: Keys
) -> None:
    s = fast(settings)
    suspect, plain = await _expired_entries(r, s, keys, [4, 1])
    claimed, _more = await Reaper(r, s, "a").reclaim(count=10, suspect_slots=0)
    assert [c.entry_id for c in claimed] == [plain]
    [left] = [p for p in await pending(r, keys.stream, s.group) if p.entry_id == suspect]
    assert left.deliveries == 4


async def test_entries_past_max_deliveries_are_never_held_back_as_suspects(
    r: aioredis.Redis, settings: Settings, keys: Keys
) -> None:
    """An entry past max_deliveries goes to the DLQ without running, so it can't crash
    anything: the reaper takes it even with no suspect slot, and the worker dead-letters it."""
    s = fast(settings, max_deliveries=3)
    [doomed] = await _expired_entries(r, s, keys, [3])
    claimed, _more = await Reaper(r, s, "a").reclaim(count=10, suspect_slots=0)
    assert [(c.entry_id, c.deliveries, c.is_suspect(s)) for c in claimed] == [(doomed, 4, False)]


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


async def test_reaper_cursor_continues_through_a_long_pel(
    r: aioredis.Redis, settings: Settings, keys: Keys
) -> None:
    """XAUTOCLAIM examines at most COUNT x 10 PEL entries per call. A stale entry behind
    25 healthy ones is only reached if the reaper keeps its cursor between passes, and
    `more` says when a pass stopped early (the worker then reaps again right away)."""
    s = with_(settings, visibility_timeout=30.0)
    client = Client(r, s)
    await r.xgroup_create(keys.stream, s.group, id="0", mkstream=True)
    for _ in range(26):
        await client.enqueue("t")
        await deliver(r, keys.stream, s.group, "alive")
    stale = (await pending(r, keys.stream, s.group))[-1].entry_id
    # Make only the LAST entry look expired: XCLAIM's IDLE option sets its idle time.
    await r.xclaim(keys.stream, s.group, "crashed", 0, [stale], idle=60_000)

    reaper = Reaper(r, s, "reaper")
    passes: list[tuple[list[str], bool]] = []
    for _ in range(3):
        claimed, more = await reaper.reclaim(count=1)
        passes.append(([c.entry_id for c in claimed], more))
    # Passes 1-2 scan 10 healthy entries each and stop early; pass 3 reaches the stale one
    # and finishes the scan.
    assert passes == [([], True), ([], True), ([stale], False)]


async def test_reclaim_reports_pending_entries_whose_data_was_deleted(
    r: aioredis.Redis, settings: Settings, keys: Keys, caplog: pytest.LogCaptureFixture
) -> None:
    """Something outside ftq deleted a pending entry's data (our exits always ack first).
    XAUTOCLAIM drops the id from the PEL, since there's nothing left to run, and the
    reaper logs it at ERROR: each one is a job no worker can ever run."""
    s = fast(settings)
    await Client(r, s).enqueue("t")
    await r.xgroup_create(keys.stream, s.group, id="0", mkstream=True)
    entry = await deliver(r, keys.stream, s.group, "crashed")
    await r.xdel(keys.stream, entry)  # data gone, still pending
    assert await pel_size(r, keys.stream, s.group) == 1

    # Let the lease expire naturally. (Faking the idle time with XCLAIM ... IDLE would not
    # work: on Redis 7+, XCLAIM itself drops a pending id whose data is gone.)
    async def expired() -> bool:
        return (await pending(r, keys.stream, s.group))[0].idle_ms >= 500

    await wait_for(expired)

    claimed, _more = await Reaper(r, s, "reaper").reclaim(count=10)

    assert claimed == []
    assert await pel_size(r, keys.stream, s.group) == 0
    errors = [rec for rec in caplog.records if rec.levelname == "ERROR"]
    assert len(errors) == 1 and entry in errors[0].getMessage()


# ---------------------------------------------------------------- cross-path races


async def test_stale_commit_after_the_reclaimer_scheduled_a_retry(
    r: aioredis.Redis, settings: Settings, keys: Keys
) -> None:
    """a stalls; b reclaims, its run fails, and b schedules a retry. Then a finishes and
    commits. The work was done, so a's commit is the (first-wins) success. The pending
    retry must not produce a second result, and if it keeps failing it must not
    overwrite SUCCEEDED."""
    job = await _setup(r, settings, keys)
    entry = await deliver(r, keys.stream, settings.group, "a")
    await _steal(r, settings, keys, entry, to="b")
    assert await _as(r, settings, "b").retry(entry, job, delay_s=0) is Outcome.OK

    assert await _as(r, settings, "a").commit(entry, job, "{}") is Commit.COMMITTED

    # The retry comes due and is delivered: a success is suppressed...
    assert await Scheduler(r, settings).move_due() == 1
    retry_entry = await deliver(r, keys.stream, settings.group, "c")
    retry_job = job.model_copy(update={"attempt": 1})
    await add_entry(r, keys.stream, (await entries(r, keys.stream))[0][1])  # a 2nd copy
    copy_entry = await deliver(r, keys.stream, settings.group, "d")
    assert await _as(r, settings, "c").commit(retry_entry, retry_job, "{}") is Commit.DUPLICATE
    # ...and a failure is dropped, not retried again or dead-lettered.
    assert await _as(r, settings, "d").retry(copy_entry, retry_job, 0) is Outcome.TERMINAL

    assert (await hash_of(r, keys.done(job.job_id)))["state"] == "SUCCEEDED"
    assert await r.xlen(keys.results) == 1
    assert await r.zcard(keys.delayed) == 0
    assert await r.xlen(keys.dead) == 0
    assert await r.xlen(keys.stream) == 0
    assert await pel_size(r, keys.stream, settings.group) == 0


async def test_stale_commit_after_the_job_was_dead_lettered_and_requeued(
    r: aioredis.Redis, settings: Settings, keys: Keys
) -> None:
    """a stalls; b reclaims and dead-letters the job; an operator requeues it. Then a
    finishes. Requeue cleared DEAD, so a's commit is a plain first success (not a late
    success), and the requeued copy becomes a suppressed duplicate. One result."""
    job = await _setup(r, settings, keys)
    entry = await deliver(r, keys.stream, settings.group, "a")
    await _steal(r, settings, keys, entry, to="b")
    b = _as(r, settings, "b")
    assert await b.dead(entry, job.job_id, DeadReason.MAX_DELIVERIES, "x", 1) is Outcome.OK
    assert await dlq.requeue(r, keys, job.job_id)

    assert await _as(r, settings, "a").commit(entry, job, "{}") is Commit.COMMITTED

    requeued_entry = await deliver(r, keys.stream, settings.group, "c")
    requeued = job.model_copy(update={"attempt": 0})
    assert await _as(r, settings, "c").commit(requeued_entry, requeued, "{}") is Commit.DUPLICATE
    assert (await hash_of(r, keys.done(job.job_id)))["state"] == "SUCCEEDED"
    assert await r.xlen(keys.results) == 1
    assert await r.xlen(keys.dead) == 0
    assert await pel_size(r, keys.stream, settings.group) == 0
