"""SPEC Phase 3: several worker PROCESSES share one queue and complete N jobs exactly
once. Real `ftq worker` subprocesses, driven by the real `ftq bench` and `ftq stats`
commands, and checked against the append-only logs (the chaos verifier's evidence)."""

import asyncio
import json
import os
import signal
import sys
from collections import Counter
from typing import Any

import pytest
import redis.asyncio as aioredis

from ftq.config import Settings
from ftq.keys import Keys

from .helpers import REPO_ROOT, entries, pel_size, start_worker_process

pytestmark = [pytest.mark.integration, pytest.mark.asyncio, pytest.mark.slow]

N = 1500
WORKERS = 3


async def _ftq(settings: Settings, *args: str) -> tuple[int, str]:
    proc = await asyncio.create_subprocess_exec(
        sys.executable,
        "-m",
        "ftq",
        *args,
        env={**os.environ, "FTQ_REDIS_URL": settings.redis_url, "FTQ_QUEUE": settings.queue},
        cwd=REPO_ROOT,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    out, err = await asyncio.wait_for(proc.communicate(), timeout=60)
    assert proc.returncode is not None
    return proc.returncode, out.decode() + err.decode()


async def test_worker_processes_complete_every_job_exactly_once(
    r: aioredis.Redis, settings: Settings, keys: Keys
) -> None:
    workers = [await start_worker_process(settings) for _ in range(WORKERS)]
    try:
        code, out = await _ftq(
            settings, "bench", "--jobs", str(N), "--batch", "200", "--timeout", "45"
        )
        assert code == 0, out
        report: dict[str, Any] = json.loads(out[out.index("{") :])
        assert (report["completed"], report["missing"], report["duplicate_results"]) == (N, 0, 0)

        code, out = await _ftq(settings, "stats")
        assert code == 0, out
        stats = json.loads(out)
        assert (stats["depth"], stats["in_flight"], stats["delayed"], stats["dlq"]) == (0, 0, 0, 0)
        assert stats["counters"]["processed"] == N
        assert stats["consumers"] == WORKERS
    finally:
        for proc, _ in workers:
            proc.send_signal(signal.SIGTERM)
        for proc, _ in workers:
            await asyncio.wait_for(proc.communicate(), timeout=15)
    assert all(proc.returncode == 0 for proc, _ in workers)

    # Independent of the bench's own check: read the append-only logs directly.
    results = await entries(r, keys.results)
    per_job = Counter(f["job_id"] for _id, f in results)
    assert len(per_job) == N and set(per_job.values()) == {1}  # no loss, no duplicates
    effects = await entries(r, keys.effects)
    assert len(effects) == N and len({f["key"] for _id, f in effects}) == N
    # The work really was shared: every worker process committed some of it.
    by_worker = Counter(f["worker_id"] for _id, f in results)
    assert set(by_worker) == {worker_id for _, worker_id in workers}
    assert await r.xlen(keys.stream) == 0 and await pel_size(r, keys.stream, settings.group) == 0
