"""A job that keeps crashing its worker ends in the DLQ via max_deliveries.

This needs workers that REALLY die, so each one is a `python -m ftq worker` subprocess,
and the built-in `crashy` handler calls `os._exit` mid-job: no commit, no retry, no
drain, just like a segfault or the OOM killer. The handler can't count its own failures
(it dies first), so the job's delivery count in the PEL is the only record. That's the
signal the poison detector uses (SPEC §4, second DLQ path).
"""

import asyncio
import signal

import pytest
import redis.asyncio as aioredis

from ftq.client import Client
from ftq.config import Settings
from ftq.keys import Keys
from ftq.metrics import read_counters

from .helpers import entries, hash_of, pel_size, start_worker_process, wait_for

pytestmark = [pytest.mark.integration, pytest.mark.asyncio, pytest.mark.slow]

CRASH_EXIT_CODE = 70
ENV = {
    "FTQ_VISIBILITY_TIMEOUT": "0.5",
    "FTQ_HEARTBEAT_INTERVAL": "0.1",
    "FTQ_REAP_INTERVAL": "0.05",
    "FTQ_MAX_DELIVERIES": "2",
}


async def test_crash_looping_job_hits_max_deliveries_and_goes_to_dlq(
    r: aioredis.Redis, settings: Settings, keys: Keys
) -> None:
    job_id = await Client(r, settings).enqueue("crashy", {"exit_code": CRASH_EXIT_CODE})

    # Deliveries 1 and 2: each worker takes the job (the first by XREADGROUP, the second
    # by reclaiming it once the dead worker's lease expires) and dies running it.
    for delivery in (1, 2):
        proc, _worker_id = await start_worker_process(settings, **ENV)
        code = await asyncio.wait_for(proc.wait(), timeout=15)
        assert code == CRASH_EXIT_CODE, f"delivery {delivery}: worker exited {code}"
        assert not await r.exists(keys.done(job_id))

    # Delivery 3 > max_deliveries (2): this worker reclaims the entry and, seeing the
    # count, sends it to the DLQ without running it. So this worker survives.
    proc, survivor = await start_worker_process(settings, **ENV)
    try:

        async def dead() -> bool:
            return (await hash_of(r, keys.done(job_id))).get("state") == "DEAD"

        await wait_for(dead, within=15)
    finally:
        proc.send_signal(signal.SIGTERM)
        _out, err = await asyncio.wait_for(proc.communicate(), timeout=15)
    assert proc.returncode == 0, err.decode()

    [(_id, fields)] = await entries(r, keys.dead)
    assert fields["dlq_job_id"] == job_id
    assert fields["dlq_reason"] == "max_deliveries"
    assert fields["dlq_deliveries"] == "3"
    assert fields["dlq_attempts"] == "1"  # one attempt, crashed on every delivery
    assert fields["dlq_worker_id"] == survivor
    counters = await read_counters(r, keys)
    assert (counters["reclaimed"], counters["dead"], counters["processed"]) == (2, 1, 0)
    assert await r.xlen(keys.stream) == 0
    assert await pel_size(r, keys.stream, settings.group) == 0
