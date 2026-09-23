"""The verifier's DLQ rules for crashy jobs (I3), on hand-built Redis state.

A crashy job must never be dead-lettered while it still had deliveries left, and must
never run past max_deliveries. It MAY be dead-lettered later than max_deliveries + 1: a
fault can interrupt the DLQ move itself (a 100K chaos run found one at 14 with
max_deliveries 12, after a reclaim's reply was lost). ADR-036.
"""

from typing import Any

import pytest
import redis.asyncio as aioredis

from chaos.verifier import verify
from ftq.config import Settings
from ftq.keys import Keys

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

MAX_DELIVERIES = 12


async def _dead_crashy(r: aioredis.Redis, s: Settings, keys: Keys, deliveries: int) -> str:
    await r.xgroup_create(keys.stream, s.group, id="0", mkstream=True)
    job_id = "crashy-1"
    fields: dict[Any, Any] = {
        "job_id": job_id,
        "dlq_job_id": job_id,
        "dlq_reason": "max_deliveries",
        "dlq_error": "delivered too often",
        "dlq_attempts": "1",
        "dlq_deliveries": str(deliveries),
    }
    dead_id = await r.xadd(keys.dead, fields)
    await r.hset(keys.done(job_id), mapping={"state": "DEAD", "dead_entry_id": str(dead_id)})
    return job_id


async def _i3(r: aioredis.Redis, s: Settings, keys: Keys, job_id: str, crashes: int) -> Any:
    report = await verify(
        r, keys, s.group, {job_id: "crashy"}, 8, MAX_DELIVERIES, {"crash_restarts": crashes}
    )
    return report["invariants"]["I3_dlq_correct"]


@pytest.mark.parametrize(("deliveries", "ok"), [(12, False), (13, True), (14, True)])
async def test_crashy_is_dead_lettered_only_after_max_deliveries(
    r: aioredis.Redis, settings: Settings, keys: Keys, deliveries: int, ok: bool
) -> None:
    job_id = await _dead_crashy(r, settings, keys, deliveries)
    i3 = await _i3(r, settings, keys, job_id, crashes=MAX_DELIVERIES)
    assert i3["ok"] is ok, i3["violations"]


async def test_crashy_that_ran_past_max_deliveries_fails_i3(
    r: aioredis.Redis, settings: Settings, keys: Keys
) -> None:
    job_id = await _dead_crashy(r, settings, keys, MAX_DELIVERIES + 1)
    i3 = await _i3(r, settings, keys, job_id, crashes=MAX_DELIVERIES + 1)  # one crash too many
    assert i3["ok"] is False
    assert "ran past max_deliveries" in i3["violations"][0]


# ---------------------------------------------------------------- I1 and I4


async def _normal(r: aioredis.Redis, s: Settings, keys: Keys, state: str | None) -> str:
    """One `normal` job that ended in `state` (None: no terminal state at all), with
    its result and effect logged if it succeeded."""
    await r.xgroup_create(keys.stream, s.group, id="0", mkstream=True)
    job_id = "normal-1"
    if state is not None:
        await r.hset(keys.done(job_id), mapping={"state": state})
    if state == "SUCCEEDED":
        await r.xadd(keys.results, {"job_id": job_id})
        await r.xadd(keys.effects, {"key": f"send_email:{job_id}"})
    return job_id


@pytest.mark.parametrize(("state", "ok"), [("SUCCEEDED", True), ("DEAD", False), (None, False)])
async def test_i1_requires_the_terminal_state_the_kind_must_end_in(
    r: aioredis.Redis, settings: Settings, keys: Keys, state: str | None, ok: bool
) -> None:
    job_id = await _normal(r, settings, keys, state)
    report = await verify(r, keys, settings.group, {job_id: "normal"}, 8, MAX_DELIVERIES)
    assert report["invariants"]["I1_no_loss"]["ok"] is ok


async def test_i1_fails_on_a_late_success(
    r: aioredis.Redis, settings: Settings, keys: Keys
) -> None:
    """Every kind that may die can never succeed, so a late success means some job was
    DEAD for a while: not allowed in a chaos run."""
    job_id = await _normal(r, settings, keys, "SUCCEEDED")
    await r.hset(keys.stats, "late_successes", 1)
    report = await verify(r, keys, settings.group, {job_id: "normal"}, 8, MAX_DELIVERIES)
    assert report["invariants"]["I1_no_loss"]["ok"] is False


async def test_i4_fails_when_a_fault_minimum_is_not_met(
    r: aioredis.Redis, settings: Settings, keys: Keys
) -> None:
    job_id = await _normal(r, settings, keys, "SUCCEEDED")
    for name in ("reclaimed", "duplicates_suppressed", "timeouts"):
        await r.hset(keys.stats, name, 1)
    enough = {"kills": 3, "pauses": 3, "network_windows": 6, "pool_resets": 1, "crash_restarts": 1}
    for evidence, ok in ((enough, True), ({**enough, "pauses": 2}, False)):
        report = await verify(
            r, keys, settings.group, {job_id: "normal"}, 8, MAX_DELIVERIES, evidence
        )
        assert report["invariants"]["I4_faults_happened"]["ok"] is ok, evidence
