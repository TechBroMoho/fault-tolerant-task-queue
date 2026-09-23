"""Jobs fetched alongside a crashing job must not follow it into the DLQ.

A `crashy` job kills its worker process, and with it every other job that worker was
running. Those jobs did nothing wrong, but their entries are now expired in the PEL right
next to the crashy one. If one reaper claims them all together, they crash the next
worker together, again and again, and each crash adds a delivery to every one of them.
When the crashy job reaches max_deliveries, so do its companions, and they are
dead-lettered with it: a false DEAD (chaos invariant I1; ADR-008, ADR-035).

The fix under test: an entry being delivered for the `suspect_deliveries`-th time or more
is a suspect, and a worker runs at most one suspect at a time. The companions then
separate from the crashy job after at most one more shared crash.

Real worker processes, because the crash is a real `os._exit`.
"""

import asyncio
import signal

import pytest
import redis.asyncio as aioredis

from ftq.client import Client, NewJob
from ftq.config import Settings
from ftq.keys import Keys

from .helpers import entries, hash_of, start_worker_process

pytestmark = [pytest.mark.integration, pytest.mark.asyncio, pytest.mark.slow]

CRASH_EXIT_CODE = 70
# suspect_deliveries stays at its default (3).
ENV = {
    "FTQ_VISIBILITY_TIMEOUT": "0.5",
    "FTQ_HEARTBEAT_INTERVAL": "0.1",
    "FTQ_REAP_INTERVAL": "0.05",
}
# Enough for the crash chain in either design (the old one needs 4 workers).
MAX_WORKERS = 6


# max_deliveries 2 is below the default suspect threshold: the Phase 4 review found that
# entries then reached the DLQ before they could ever count as suspects (ADR-035).
@pytest.mark.parametrize("max_deliveries", [3, 2])
@pytest.mark.parametrize("crashy_position", ["first", "last"])
async def test_jobs_fetched_with_a_crashing_job_are_not_dead_lettered_with_it(
    r: aioredis.Redis, settings: Settings, keys: Keys, crashy_position: str, max_deliveries: int
) -> None:
    env = {**ENV, "FTQ_MAX_DELIVERIES": str(max_deliveries)}
    # One batch, so the first worker fetches all five in one XREADGROUP. The innocents
    # have some latency, so they are all mid-run (awaiting) when the crashy job kills the
    # process. The crashy job's position decides whether it is the first or the last of
    # the expired entries in PEL (id) order, i.e. which one a reaper meets first.
    innocents = [NewJob("send_email", {"latency_ms": 300}) for _ in range(4)]
    crashy = NewJob("crashy", {"exit_code": CRASH_EXIT_CODE})
    batch = [crashy, *innocents] if crashy_position == "first" else [*innocents, crashy]
    job_ids = await Client(r, settings).enqueue_many(batch)
    crashy_id = job_ids[batch.index(crashy)]
    innocent_ids = [j for j in job_ids if j != crashy_id]

    async def states() -> dict[str, str]:
        return {j: (await hash_of(r, keys.done(j))).get("state", "") for j in job_ids}

    # Start workers one after another. Each one either dies (crashy) or survives; a
    # survivor runs until every job has a terminal state.
    crashes = 0
    survivor: asyncio.subprocess.Process | None = None
    for _ in range(MAX_WORKERS):
        proc, _worker_id = await start_worker_process(settings, env=env)
        exited = asyncio.ensure_future(proc.wait())
        deadline = asyncio.get_running_loop().time() + 20
        while not exited.done() and asyncio.get_running_loop().time() < deadline:
            if all((await states()).values()):
                break
            await asyncio.sleep(0.05)
        if exited.done():
            assert exited.result() == CRASH_EXIT_CODE
            crashes += 1
            continue
        exited.cancel()
        survivor = proc
        break
    assert survivor is not None, f"every one of {MAX_WORKERS} workers crashed"
    survivor.send_signal(signal.SIGTERM)
    _out, err = await asyncio.wait_for(survivor.communicate(), timeout=15)
    assert survivor.returncode == 0, err.decode()

    final = await states()
    assert {j: final[j] for j in innocent_ids} == dict.fromkeys(innocent_ids, "SUCCEEDED")
    assert final[crashy_id] == "DEAD"
    [(_id, dead)] = await entries(r, keys.dead)
    assert (dead["dlq_job_id"], dead["dlq_reason"]) == (crashy_id, "max_deliveries")
    # It killed max_deliveries workers and was dead-lettered unrun on the next delivery.
    assert dead["dlq_deliveries"] == str(max_deliveries + 1)
    assert crashes == max_deliveries
    # Each innocent's email went out exactly once.
    effects = [f["key"] for _id, f in await entries(r, keys.effects)]
    assert sorted(effects) == sorted(f"send_email:{j}" for j in innocent_ids)
