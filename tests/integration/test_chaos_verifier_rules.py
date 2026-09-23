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
