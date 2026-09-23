"""The load generator (bench/loadgen.py) against a real Redis and a real worker.

Short runs, but the full pipeline: spawned producer processes, open-loop pacing, the
Redis-TIME window, the results-log analysis, and the exactly-once verdict. The last two
tests check that the report can't claim more than happened: a run that didn't drain
isn't "exactly once", and a refused job is counted as refused, not as accepted.
"""

import asyncio
from typing import Any

import pytest
import redis.asyncio as aioredis

from bench.loadgen import LoadSpec, loadgen_key, produce_only, run
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


# ---------------------------------------------------------------- several loadgen hosts
# Phase 8 drives the queue from 2 loadgen hosts (ADR-046). One coordinator measures; the
# other hosts only produce. The exactly-once check must count every host's accepted jobs,
# and a run it can't fully account for must never pass.


async def test_two_hosts_offer_one_rate_and_the_check_counts_both(
    r: aioredis.Redis, settings: Settings
) -> None:
    spec = _spec(settings, rate=1000, hosts=2, run_id="two-hosts", expect_workers=1)
    async with running_worker(r, settings, registry):
        # The second host gets only the URL, the queue, and the run id: everything else
        # (rate, processes, window) comes from the coordinator's published spec.
        remote = asyncio.create_task(
            produce_only(settings.redis_url, settings.queue, "two-hosts", host_timeout=30)
        )
        report = await run(spec, {"label": "test"})
        await remote

    p = report["producers"]
    assert p["hosts"] == {"expected": 2, "reported": 2}
    assert len(p["cpu_busy"]) == 4  # 2 processes on each host
    once = report["exactly_once"]
    assert once["ok"], once
    # 1000/s in total, split across both hosts: 4 s -> ~4000, from both hosts' counts.
    assert 3980 <= once["accepted"] <= 4000
    assert once["results"] == once["distinct_jobs_with_results"] == once["accepted"]
    assert report["throughput"]["offered_per_s"] == pytest.approx(1000, rel=0.02)
    assert p["max_start_late_s"] < 0.5  # both hosts started together


async def test_a_host_that_never_joins_stops_the_run_before_it_enqueues(
    r: aioredis.Redis, settings: Settings
) -> None:
    spec = _spec(settings, rate=1000, hosts=2, run_id="no-show", host_timeout=1)
    with pytest.raises(RuntimeError, match="1 of 2 loadgen hosts"):
        await run(spec, {})
    assert await r.xlen(Keys(settings.queue).stream) == 0


async def test_a_host_that_joins_but_never_reports_fails_the_check(
    r: aioredis.Redis, settings: Settings
) -> None:
    spec = _spec(settings, rate=500, hosts=2, run_id="crashed", host_timeout=2)
    # A host that registered and then died: ready, but no summary will ever come.
    await r.rpush(loadgen_key(settings.queue, "crashed", "ready"), "ghost-host")
    async with running_worker(r, settings, registry):
        report = await run(spec, {})
    assert report["producers"]["hosts"] == {"expected": 2, "reported": 1}
    once = report["exactly_once"]
    # This host's own jobs all completed exactly once, but the other host's accepted
    # count is unknown, so "nothing missing" can't be claimed.
    assert once["missing"] == 0
    assert not once["ok"]
