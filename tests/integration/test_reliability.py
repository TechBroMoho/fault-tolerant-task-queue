"""Phase 2 acceptance, end to end through running workers (SPEC §7 Phase 2).

Workers here run in-process with a 0.5 s lease (helpers.FAST). The crash-loop case needs
a worker that really dies, so it lives in test_crash_loop.py with real subprocesses.
"""

import asyncio
import json
from typing import Any

import pytest
import redis.asyncio as aioredis

from ftq.client import Client
from ftq.config import Settings
from ftq.handlers import registry as builtin_registry
from ftq.keys import Keys
from ftq.metrics import read_counters
from ftq.models import Job
from ftq.registry import JobContext, Registry

from .helpers import (
    add_entry,
    deliver,
    entries,
    fast,
    hash_of,
    pel_size,
    pending,
    running_worker,
    wait_for,
    watching_lease,
)

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


async def _state(r: aioredis.Redis, keys: Keys, job_id: str) -> str | None:
    return (await hash_of(r, keys.done(job_id))).get("state")


async def _wait_state(
    r: aioredis.Redis, keys: Keys, job_id: str, state: str, within: float = 10
) -> None:
    async def reached() -> bool:
        return await _state(r, keys, job_id) == state

    await wait_for(reached, within=within)


async def _assert_drained(r: aioredis.Redis, keys: Keys, group: str) -> None:
    """Nothing left anywhere a job could be waiting (invariant I5, in miniature)."""
    assert await r.xlen(keys.stream) == 0
    assert await pel_size(r, keys.stream, group) == 0
    assert await r.zcard(keys.delayed) == 0


async def test_job_of_a_killed_worker_is_reclaimed_and_completed_once(
    r: aioredis.Redis, settings: Settings, keys: Keys
) -> None:
    """A worker took the job and died (never acked, never heartbeat). After the lease
    expires, a live worker reclaims it and completes it exactly once."""
    s = fast(settings)
    job_id = await Client(r, s).enqueue("send_email", {"to": "ada@example.com"})
    await r.xgroup_create(keys.stream, s.group, id="0", mkstream=True)
    await deliver(r, keys.stream, s.group, "killed-worker")

    async with running_worker(r, s, builtin_registry):
        await _wait_state(r, keys, job_id, "SUCCEEDED")

    assert (await hash_of(r, keys.done(job_id)))["worker_id"] == "test-worker"
    counters = await read_counters(r, keys)
    assert (counters["reclaimed"], counters["processed"]) == (1, 1)
    assert [f["job_id"] for _id, f in await entries(r, keys.results)] == [job_id]
    assert [f["key"] for _id, f in await entries(r, keys.effects)] == [f"send_email:{job_id}"]
    await _assert_drained(r, keys, s.group)


async def test_flaky_job_eventually_succeeds(
    r: aioredis.Redis, settings: Settings, keys: Keys
) -> None:
    s = fast(settings, max_attempts=5)
    job_id = await Client(r, s).enqueue("flaky", {"fail_times": 2})

    async with running_worker(r, s, builtin_registry):
        await _wait_state(r, keys, job_id, "SUCCEEDED")

    # Attempts 0 and 1 raised; attempt 2 succeeded and applied the effect once.
    result = json.loads((await hash_of(r, keys.done(job_id)))["result"])
    assert result == {"attempt": 2, "applied_now": True}
    counters = await read_counters(r, keys)
    assert (counters["retried"], counters["scheduled"], counters["processed"]) == (2, 2, 1)
    assert counters["dead"] == 0
    assert await r.xlen(keys.effects) == 1
    assert await r.xlen(keys.dead) == 0
    await _assert_drained(r, keys, s.group)


async def test_poison_job_lands_in_dlq_with_correct_attempts(
    r: aioredis.Redis, settings: Settings, keys: Keys
) -> None:
    s = fast(settings, max_attempts=3)
    job_id = await Client(r, s).enqueue("poison")

    async with running_worker(r, s, builtin_registry):
        await _wait_state(r, keys, job_id, "DEAD")

    [(dead_id, fields)] = await entries(r, keys.dead)
    assert fields["dlq_job_id"] == job_id
    assert fields["dlq_reason"] == "max_attempts"
    assert fields["dlq_attempts"] == "3"  # = max_attempts: 1 try + 2 retries
    assert fields["attempt"] == "2"  # the job's own 0-based attempt number when it died
    assert fields["dlq_error"] == "InjectedFailure: poison: this job always fails"
    done = await hash_of(r, keys.done(job_id))
    assert (done["state"], done["dead_entry_id"]) == ("DEAD", dead_id)
    counters = await read_counters(r, keys)
    assert (counters["retried"], counters["dead"], counters["processed"]) == (2, 1, 0)
    assert await r.xlen(keys.effects) == 0  # a poison job never applies its effect
    assert await r.xlen(keys.results) == 0
    await _assert_drained(r, keys, s.group)


async def test_malformed_and_unknown_jobs_go_straight_to_dlq(
    r: aioredis.Redis, settings: Settings, keys: Keys
) -> None:
    """Neither can ever succeed, so retrying would only burn attempts."""
    s = fast(settings)
    unknown = await Client(r, s).enqueue("no_such_type")
    await add_entry(r, keys.stream, {"job_id": "broken", "type": "send_email"})  # no payload

    async with running_worker(r, s, builtin_registry):
        await _wait_state(r, keys, unknown, "DEAD")
        await _wait_state(r, keys, "broken", "DEAD")

    dead = {f["dlq_job_id"]: f for _id, f in await entries(r, keys.dead)}
    assert {j: f["dlq_reason"] for j, f in dead.items()} == {
        unknown: "unknown_type",
        "broken": "malformed",
    }
    # "Straight to": first delivery, no handler run, no retry.
    for fields in dead.values():
        assert (fields["dlq_attempts"], fields["dlq_deliveries"]) == ("0", "1")
    assert (await read_counters(r, keys))["retried"] == 0
    await _assert_drained(r, keys, s.group)


@pytest.mark.slow  # > 1 s: runs in `make test-all` and CI
async def test_long_job_with_heartbeats_is_not_reclaimed(
    r: aioredis.Redis, settings: Settings, keys: Keys
) -> None:
    """A 2 s job under a 0.5 s lease, with a second worker's reaper armed throughout."""
    s = fast(settings)
    job_id = await Client(r, s).enqueue("send_email", {"latency_ms": 2000})

    async with running_worker(r, s, builtin_registry, worker_id="A"):

        async def owned_by_a() -> bool:
            return [p.owner for p in await pending(r, keys.stream, s.group)] == ["A"]

        await wait_for(owned_by_a)
        async with (
            running_worker(r, s, builtin_registry, worker_id="B"),
            watching_lease(r, keys.stream, s.group) as watch,
        ):
            await _wait_state(r, keys, job_id, "SUCCEEDED")

    assert (await hash_of(r, keys.done(job_id)))["worker_id"] == "A"
    assert watch.owners == {"A"} and watch.deliveries == {1}
    assert watch.max_idle_ms < 500, watch
    counters = await read_counters(r, keys)
    assert counters["reclaimed"] == 0
    assert counters["heartbeats"] >= 10  # ~every 0.1 s for 2 s
    assert counters["duplicates_suppressed"] == 0


@pytest.mark.slow  # > 1 s: runs in `make test-all` and CI
async def test_long_job_without_heartbeats_is_reclaimed_but_its_effect_happens_once(
    r: aioredis.Redis, settings: Settings, keys: Keys
) -> None:
    """The other side of the lease (ADR-007): a `slow` job doesn't heartbeat, so the
    reaper takes it while the first run is still going. Both runs finish; commit is
    first-wins and the ledger suppresses the second effect."""
    s = fast(settings, concurrency=2)  # 2 slots: the worker can reclaim its own entry once
    job_id = await Client(r, s).enqueue("slow", {"seconds": 1.0})

    async with running_worker(r, s, builtin_registry):

        async def both_runs_finished() -> bool:
            return (await read_counters(r, keys))["duplicates_suppressed"] == 1

        await wait_for(both_runs_finished)

    assert await _state(r, keys, job_id) == "SUCCEEDED"
    counters = await read_counters(r, keys)
    assert (counters["reclaimed"], counters["processed"]) == (1, 1)
    assert (counters["effects_applied"], counters["effects_suppressed"]) == (1, 1)
    assert await r.xlen(keys.results) == 1
    await _assert_drained(r, keys, s.group)


@pytest.mark.parametrize("a_resumes_by", ["raising_retry", "raising_dlq", "committing"])
async def test_stale_worker(
    r: aioredis.Redis, settings: Settings, keys: Keys, a_resumes_by: str
) -> None:
    """SPEC Phase 2 stale-worker test. Worker A takes a job and stalls past its lease (no
    heartbeats, like a GC pause or `docker pause`). Worker B reclaims it and commits.
    Then A resumes and either raises (its retry, or its DLQ move once attempts run out,
    must return LEASE_LOST and change nothing) or commits (suppressed as a duplicate).
    Either way: one terminal state, one result, one effect."""
    max_attempts = 1 if a_resumes_by == "raising_dlq" else 5
    s = fast(settings, max_attempts=max_attempts)
    resume_a = asyncio.Event()
    registry = Registry()

    @registry.register("job", heartbeat=False)  # a stalled worker sends no heartbeats
    async def job(ctx: JobContext) -> str:
        if ctx.worker_id == "A":
            await resume_a.wait()  # the stall
            if a_resumes_by.startswith("raising"):
                raise RuntimeError("A's handler failed after its lease was lost")
        await ctx.ledger.apply(f"job:{ctx.job.job_id}")
        return ctx.worker_id

    job_id = await Client(r, s).enqueue("job")
    # A has one slot, and it's busy, so A's own reaper can't take the job back.
    async with running_worker(r, fast(s, concurrency=1), registry, worker_id="A"):

        async def a_lease_expired() -> bool:
            pel = await pending(r, keys.stream, s.group)
            return [p.owner for p in pel] == ["A"] and pel[0].idle_ms >= 500

        await wait_for(a_lease_expired)
        async with running_worker(r, s, registry, worker_id="B"):
            await _wait_state(r, keys, job_id, "SUCCEEDED")
            assert (await hash_of(r, keys.done(job_id)))["worker_id"] == "B"

            resume_a.set()
            key = "duplicates_suppressed" if a_resumes_by == "committing" else "lease_lost"

            async def a_finished() -> bool:
                return (await read_counters(r, keys))[key] == 1

            await wait_for(a_finished)

    counters = await read_counters(r, keys)
    assert await _state(r, keys, job_id) == "SUCCEEDED"  # exactly one terminal state...
    assert (await hash_of(r, keys.done(job_id)))["worker_id"] == "B"  # ...B's
    assert await r.xlen(keys.results) == 1  # one result
    assert await r.xlen(keys.effects) == 1  # one effect
    assert await r.xlen(keys.dead) == 0  # A's DLQ move (raising_dlq) did nothing
    assert (counters["retried"], counters["dead"]) == (0, 0)  # A's retry did nothing
    assert counters["processed"] == 1
    await _assert_drained(r, keys, s.group)


@pytest.mark.slow  # > 1 s: runs in `make test-all` and CI
async def test_heartbeats_stop_after_the_lease_is_lost(
    r: aioredis.Redis, settings: Settings, keys: Keys
) -> None:
    """A is running a heartbeating job when another worker takes the entry over (as a
    reaper would after A stalled). A's next heartbeat is refused. A stops heartbeating
    and doesn't take the entry back, so the lease stays with the new owner."""
    s = fast(settings)
    release = asyncio.Event()
    registry = Registry()

    @registry.register("job")
    async def job(ctx: JobContext) -> None:
        await release.wait()

    job_id = await Client(r, s).enqueue("job")
    # One slot, busy with this job: A's own reaper can't take the entry back from B (which,
    # being a bare consumer name here, never heartbeats). Only A's heartbeat could.
    async with running_worker(r, fast(s, concurrency=1), registry, worker_id="A"):

        async def a_heartbeating() -> bool:
            return (await read_counters(r, keys))["heartbeats"] >= 2

        await wait_for(a_heartbeating)
        [p] = await pending(r, keys.stream, s.group)
        await r.xclaim(keys.stream, s.group, "B", 0, [p.entry_id])  # B's reclaim

        async def a_saw_lease_lost() -> bool:
            return (await read_counters(r, keys))["lease_lost"] == 1

        await wait_for(a_saw_lease_lost)
        beats = (await read_counters(r, keys))["heartbeats"]
        async with watching_lease(r, keys.stream, s.group) as watch:
            # Watch for ~8 heartbeat intervals: B must stay the owner throughout.
            for _ in range(8):
                await asyncio.sleep(s.heartbeat_interval)
        assert watch.owners == {"B"}
        counters = await read_counters(r, keys)
        assert counters["heartbeats"] == beats  # A stopped
        assert counters["lease_lost"] == 1  # after one refusal
        release.set()
        await _wait_state(r, keys, job_id, "SUCCEEDED")  # commit is first-wins: fine


async def test_worker_schedules_retry_within_the_backoff_bound(
    r: aioredis.Redis, settings: Settings, keys: Keys
) -> None:
    """The unit tests bound the backoff formula; this checks the worker actually applies
    it. After attempt 0 fails, the retry's due time must be at most base x 2^0 = 2 s past
    the failure, and the job must not run again before it's due."""
    s = fast(settings, job_backoff_base=2.0, job_backoff_cap=2.0)
    before_ms = int((await r.time())[0]) * 1000
    job_id = await Client(r, s).enqueue("flaky", {"fail_times": 1})
    async with running_worker(r, s, builtin_registry):

        async def retry_parked() -> bool:
            return bool(await r.zcard(keys.delayed))

        await wait_for(retry_parked)
        secs, micros = await r.time()
        after_ms = int(secs) * 1000 + int(micros) // 1000
        delayed: Any = await r.zrange(keys.delayed, 0, -1, withscores=True)
        [(_member, due_ms)] = delayed
        assert before_ms <= due_ms <= after_ms + 2000
        if due_ms > after_ms + 100:  # comfortably in the future: must not have run yet
            assert (await read_counters(r, keys))["scheduled"] == 0
        await _wait_state(r, keys, job_id, "SUCCEEDED")
    assert (await read_counters(r, keys))["scheduled"] == 1


async def test_scheduler_drains_a_backlog_without_waiting_between_full_batches(
    r: aioredis.Redis, settings: Settings, keys: Keys
) -> None:
    """ADR-026: a full batch means more may be due, so the mover goes again at once. With a
    30 s scheduler interval and a batch of 5, 23 due retries are moved in well under a
    second. If it slept after every batch, only the first 5 would move."""
    s = fast(settings, scheduler_interval=30.0, scheduler_batch=5)
    now_ms = int((await r.time())[0]) * 1000
    members = {}
    for i in range(23):
        fields = {
            **Job(job_id=f"backlog-{i}", type="send_email").to_fields(),
            "enqueued_at_ms": str(now_ms),
        }
        flat = [x for name_value in fields.items() for x in name_value]  # retry.lua's format
        members[json.dumps(flat)] = now_ms - 1000  # due a second ago
    await r.zadd(keys.delayed, members)

    async with running_worker(r, s, builtin_registry):

        async def all_moved() -> bool:
            return (await read_counters(r, keys))["scheduled"] == 23

        await wait_for(all_moved, within=2)

        async def all_done() -> bool:
            return (await read_counters(r, keys))["processed"] == 23

        await wait_for(all_done)
    await _assert_drained(r, keys, s.group)
