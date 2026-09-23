"""The load generator (bench/loadgen.py) against a real Redis and a real worker.

Short runs, but the full pipeline: spawned producer processes, open-loop pacing, the
Redis-TIME window, the results-log analysis, and the exactly-once verdict. The last two
tests check that the report can't claim more than happened: a run that didn't drain
isn't "exactly once", and a refused job is counted as refused, not as accepted.
"""

from typing import Any

import pytest
import redis.asyncio as aioredis

from bench.loadgen import LoadSpec, run
from ftq.config import Settings
from ftq.handlers import registry
from ftq.keys import Keys

from .helpers import running_worker

pytestmark = [pytest.mark.integration, pytest.mark.asyncio, pytest.mark.slow]


def _spec(settings: Settings, **kw: Any) -> LoadSpec:
    base: dict[str, Any] = {
        "redis_url": settings.redis_url,
        "queue": settings.queue,
        "warmup": 1,
        "measure": 2,
        "cooldown": 1,
        "processes": 2,
        "drain_timeout": 20,
    }
    return LoadSpec(**{**base, **kw})


async def test_open_loop_run_reports_rate_latency_and_exactly_once(
    r: aioredis.Redis, settings: Settings
) -> None:
    spec = _spec(settings, rate=1000, expect_workers=1)
    async with running_worker(r, settings, registry):
        report = await run(spec, {"label": "test"})

    once = report["exactly_once"]
    assert once["ok"], once
    # 4 s at 1000/s from 2 processes (the last partial tick may be cut: >= 3990).
    assert 3990 <= once["accepted"] <= 4000
    assert once["results"] == once["distinct_jobs_with_results"] == once["accepted"]
    assert report["window"]["s"] == 2.0
    # Open loop: the offered rate inside the window is the target, whatever the queue does.
    assert report["throughput"]["offered_per_s"] == pytest.approx(1000, rel=0.02)
    # A worker at concurrency 10 keeps up with 1000/s: completions track the offer.
    assert report["throughput"]["completed_per_s"] == pytest.approx(1000, rel=0.1)
    lat = report["e2e_latency_ms"]
    assert lat["count"] == pytest.approx(2000, rel=0.02)  # jobs enqueued in the window
    assert 0 <= lat["p50"] <= lat["p99"] <= lat["max"]
    # The stored histogram reproduces the summary exactly.
    hist = {int(k): v for k, v in report["histograms"]["e2e_latency_ms"].items()}
    assert sum(hist.values()) == lat["count"]
    assert report["redis"]["main_thread_busy"] is not None
    assert report["drain"]["drained"]
    assert report["counters"]["processed"] == once["accepted"]


async def test_saturation_mode_bounds_the_backlog(r: aioredis.Redis, settings: Settings) -> None:
    # No worker: nothing drains, so only the producers' depth guard stops the growth.
    spec = _spec(
        settings, rate=0, max_depth=300, batch=50, warmup=0, measure=1, cooldown=0,
        drain_timeout=0.5,
    )  # fmt: skip
    report = await run(spec, {})
    depth = await r.xlen(Keys(settings.queue).stream)
    # Each producer checks the depth before a send, so the overshoot is at most one
    # batch per producer.
    assert 300 <= depth <= 300 + 2 * 50
    assert report["producers"]["accepted"] == depth
    assert report["producers"]["depth_waits"] > 0


async def test_a_run_that_did_not_drain_is_not_exactly_once_and_refusals_count(
    r: aioredis.Redis, settings: Settings
) -> None:
    # No worker, reject mode: the queue fills to the high watermark and stays full.
    spec = _spec(
        settings, rate=500, high_watermark=100, low_watermark=50, warmup=0, measure=1,
        cooldown=0, drain_timeout=0.5,
    )  # fmt: skip
    report = await run(spec, {})
    p = report["producers"]
    assert p["accepted"] == 100  # admitted up to the high watermark, never past it
    assert p["rejected"] == p["offered"] - 100 > 0
    assert report["counters"]["rejected"] == p["rejected"]  # the script counted each one
    assert report["throughput"]["accepted_per_s"] < report["throughput"]["offered_per_s"]
    assert not report["drain"]["drained"]
    once = report["exactly_once"]
    assert not once["ok"]
    assert once["missing"] == 100
    assert await r.xlen(Keys(settings.queue).stream) == 100
