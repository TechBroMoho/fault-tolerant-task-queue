"""`ftq bench`: enqueue N jobs, wait for the workers to finish them, and check the result.

A smoke-scale load test for the Phase 3 demo (`make up WORKERS=4` + `ftq bench --jobs
50000`). It's not the Phase 6 benchmark: no steady-state window, no latency percentiles,
one producer. What it does report honestly:

- enqueue rate: N / time spent in `enqueue_many` (pipelined batches);
- drain throughput: N / time from the first enqueue until the last of these jobs is
  committed (so it includes the enqueue time: workers start as soon as jobs arrive);
- exactly-once: every one of the N job_ids appears exactly once in the append-only
  results log (the same evidence the chaos verifier uses, ADR-021).
"""

import asyncio
import json
import time
from collections import Counter
from typing import Any

import redis.asyncio as aioredis

from ftq.client import Client, NewJob
from ftq.config import Settings
from ftq.keys import Keys
from ftq.metrics import snapshot


async def run_bench(
    redis: aioredis.Redis,
    settings: Settings,
    jobs: int,
    job_type: str,
    payload: dict[str, Any],
    batch: int,
    wait_limit: float,
) -> dict[str, Any]:
    keys = Keys(settings.queue)
    # Results-log position before we start: only entries after it are ours to count.
    last: Any = await redis.xrevrange(keys.results, count=1)
    start_id = last[0][0] if last else "0-0"
    client = Client(redis, settings)

    t0 = time.monotonic()
    job_ids: list[str] = []
    for i in range(0, jobs, batch):
        n = min(batch, jobs - i)
        job_ids += await client.enqueue_many([NewJob(job_type, payload) for _ in range(n)])
    enqueue_s = time.monotonic() - t0

    # Wait until every one of our jobs has a first commit in the results log.
    ours = set(job_ids)
    seen: Counter[str] = Counter()
    completed = 0
    cursor = start_id
    deadline = t0 + wait_limit  # returns with `missing` > 0 rather than hang
    while completed < len(ours) and time.monotonic() < deadline:
        reply: Any = await redis.xrange(keys.results, min=f"({cursor}", count=10_000)
        if not reply:
            await asyncio.sleep(0.05)
            continue
        for entry_id, fields in reply:
            job_id = fields["job_id"]
            seen[job_id] += 1
            if seen[job_id] == 1 and job_id in ours:
                completed += 1
            cursor = entry_id
    drain_s = time.monotonic() - t0

    return {
        "jobs": jobs,
        "job_type": job_type,
        "payload": payload,
        "batch": batch,
        "enqueue_seconds": round(enqueue_s, 3),
        "enqueue_per_second": round(jobs / enqueue_s, 1),
        "drain_seconds": round(drain_s, 3),
        "throughput_per_second": round(completed / drain_s, 1),
        "completed": completed,
        "missing": len(ours) - completed,  # nonzero only if the timeout ran out
        "duplicate_results": sum(1 for j in ours if seen[j] > 1),
        "producer": vars(client.counters),
        "stats": await snapshot(redis, settings),
    }


def format_report(report: dict[str, Any]) -> str:
    return json.dumps(report, indent=2)
