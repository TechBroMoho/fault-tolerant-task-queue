"""Measure where batching/pipelining matters (SPEC Phase 3), before deciding to build it.

Two measurements against the local Redis (`make up`), each repeated and reported as the
median with the range:

1. Enqueue: one `enqueue()` per job (one round trip each, the "before") vs
   `enqueue_many()` pipelines of 10 / 100 / 500 jobs (the "after").
2. Send cost by batch size: the time of one pipelined round trip of n enqueue scripts
   (argument building excluded), median of 35 sends after 5 warm-ups.
3. Worker drain: one in-process worker with the whole backlog already enqueued,
   at concurrency 1 / 10 / 50, plus the worker process's CPU time over the drain.
   If the worker is CPU-bound at high concurrency, round trips are already overlapped
   and batching commits would only shave per-command overhead.

Usage:  uv run python bench/pipelining.py [--jobs 20000] [--repeats 3]
Output: a table on stdout (committed as results/local/pipelining.txt) and raw JSON
        (results/local/pipelining.json).
"""

import argparse
import asyncio
import json
import platform
import resource
import secrets
import statistics
import time
from pathlib import Path
from typing import Any

import redis.asyncio as aioredis

from ftq.client import Client, NewJob
from ftq.config import Settings, make_redis
from ftq.handlers import registry
from ftq.keys import Keys
from ftq.metrics import read_counters
from ftq.worker import Worker

RESULTS = Path(__file__).resolve().parents[1] / "results" / "local"


def _settings(**overrides: Any) -> Settings:
    return Settings(
        queue=f"bench-{secrets.token_hex(4)}",
        done_ttl_seconds=0,
        high_watermark=10_000_000,
        low_watermark=0,
        log_level="WARNING",
        **overrides,
    )


async def _cleanup(r: aioredis.Redis, s: Settings) -> None:
    doomed = [k async for k in r.scan_iter(match=f"{Keys(s.queue).prefix}:*")]
    for i in range(0, len(doomed), 500):
        await r.delete(*doomed[i : i + 500])


async def enqueue_rate(jobs: int, batch: int) -> float:
    s = _settings()
    r = make_redis(s)
    try:
        client = Client(r, s)
        t0 = time.perf_counter()
        if batch == 1:
            for _ in range(jobs):
                await client.enqueue("send_email")
        else:
            for _ in range(0, jobs, batch):
                await client.enqueue_many([NewJob("send_email") for _ in range(batch)])
        elapsed = time.perf_counter() - t0
        assert await r.xlen(Keys(s.queue).stream) == jobs
        return jobs / elapsed
    finally:
        await _cleanup(r, s)
        await r.aclose()


async def send_cost_ms(batch: int, sends: int = 40, warmup: int = 5) -> list[float]:
    s = _settings()
    r = make_redis(s)
    try:
        client = Client(r, s)
        await client.enqueue("send_email")  # load the script
        times = []
        for _ in range(sends):
            calls = [client._args(NewJob("send_email")) for _ in range(batch)]
            t0 = time.perf_counter()
            await client._send(calls, "none")
            times.append((time.perf_counter() - t0) * 1000)
        return times[warmup:]
    finally:
        await _cleanup(r, s)
        await r.aclose()


async def drain_rate(jobs: int, concurrency: int) -> tuple[float, float]:
    """(jobs/s, worker CPU seconds per wall second) for draining a ready backlog."""
    s = _settings(concurrency=concurrency, block_ms=50)
    r = make_redis(s)
    try:
        client = Client(r, s)
        for _ in range(0, jobs, 500):
            await client.enqueue_many([NewJob("send_email") for _ in range(500)])
        worker = Worker(r, s, registry)
        cpu0 = resource.getrusage(resource.RUSAGE_SELF)
        t0 = time.perf_counter()
        task = asyncio.create_task(worker.run())
        keys = Keys(s.queue)
        while (await read_counters(r, keys))["processed"] < jobs:  # noqa: ASYNC110 (polls Redis)
            await asyncio.sleep(0.01)
        elapsed = time.perf_counter() - t0
        cpu1 = resource.getrusage(resource.RUSAGE_SELF)
        worker.request_stop()
        await task
        cpu = (cpu1.ru_utime - cpu0.ru_utime) + (cpu1.ru_stime - cpu0.ru_stime)
        return jobs / elapsed, cpu / elapsed
    finally:
        await _cleanup(r, s)
        await r.aclose()


def _summary(values: list[float]) -> dict[str, float]:
    return {
        "median": round(statistics.median(values), 1),
        "min": round(min(values), 1),
        "max": round(max(values), 1),
    }


async def main(jobs: int, repeats: int) -> None:
    raw: dict[str, Any] = {
        "jobs": jobs,
        "repeats": repeats,
        "machine": f"{platform.system()} {platform.machine()} (local laptop, Redis in Docker)",
        "python": platform.python_version(),
        "enqueue": {},
        "send_cost_ms": {},
        "drain": {},
    }
    for batch in (1, 10, 100, 500):
        rates = [await enqueue_rate(jobs, batch) for _ in range(repeats)]
        raw["enqueue"][str(batch)] = {"jobs_per_s": rates}
    for batch in (1, 10, 50, 100, 200, 300, 500, 1000):
        raw["send_cost_ms"][str(batch)] = await send_cost_ms(batch)
    for concurrency in (1, 10, 50):
        runs = [await drain_rate(jobs, concurrency) for _ in range(repeats)]
        raw["drain"][str(concurrency)] = {
            "jobs_per_s": [x for x, _ in runs],
            "cpu_utilization": [c for _, c in runs],
        }

    lines = [
        f"ftq pipelining measurement: {jobs} send_email jobs, {repeats} runs each, "
        "median [min-max]",
        f"{raw['machine']}, Python {raw['python']}",
        "",
        "enqueue (one producer)          jobs/s",
    ]
    base = statistics.median(raw["enqueue"]["1"]["jobs_per_s"])
    for batch, v in raw["enqueue"].items():
        s = _summary(v["jobs_per_s"])
        label = "enqueue() one at a time" if batch == "1" else f"enqueue_many, batch {batch}"
        lines.append(
            f"  {label:<30}{s['median']:>9} [{s['min']}-{s['max']}]  x{s['median'] / base:.1f}"
        )
    lines += ["", "one pipelined send of n enqueues   median ms   p10 ms   us per job"]
    for batch, times in raw["send_cost_ms"].items():
        med = statistics.median(times)
        p10 = statistics.quantiles(times, n=10)[0]
        lines.append(f"  n = {batch:<30}{med:>8.2f} {p10:>8.2f} {med / int(batch) * 1000:>10.1f}")
    lines += ["", "worker drain (one process)      jobs/s              worker CPU / wall"]
    for conc, v in raw["drain"].items():
        s = _summary(v["jobs_per_s"])
        cpu = statistics.median(v["cpu_utilization"])
        lines.append(
            f"  concurrency {conc:<18}{s['median']:>9} [{s['min']}-{s['max']}]   {cpu:.2f}"
        )
    report = "\n".join(lines)
    print(report)
    RESULTS.mkdir(parents=True, exist_ok=True)
    (RESULTS / "pipelining.json").write_text(json.dumps(raw, indent=2) + "\n")
    (RESULTS / "pipelining.txt").write_text(report + "\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--jobs", type=int, default=20_000)
    parser.add_argument("--repeats", type=int, default=3)
    args = parser.parse_args()
    asyncio.run(main(args.jobs, args.repeats))
