"""The load generator: offer load to a running queue, measure it, and write one report.

    python -m bench.loadgen --redis-url URL --rate 20000 --warmup 10 --measure 30 ...

It needs only a Redis URL and workers already consuming the queue. It doesn't care
whether they are local containers (bench/run.py) or ECS tasks (Phase 8). Everything it
reports is read from Redis or measured in its own processes, so the same program is the
benchmark in both places. One run does four things:

1. **Produce.** `--processes` producer processes each offer `rate / processes` jobs/s,
   open loop: every tick (10 ms) a producer sends the jobs that have come due since its
   last send, as one pipelined `enqueue_many` (ADR-032). If a send is slow, the next
   batch is bigger, so a slow queue doesn't lower the offered rate. That's the
   coordinated-omission trap of closed-loop generators. `--rate 0` sends batches back to
   back instead. With `--max-depth D`, a producer holds off while the queue is D deep.
   That's the saturation mode: the workers always have a backlog and never wait for
   work, but the backlog (and Redis's memory) stays bounded, and Redis spends nothing on
   refused enqueues (ADR-041).
2. **Sample.** Every second the coordinator records the queue depth, the counters, Redis
   CPU (`INFO cpu`), and memory, all against Redis `TIME`.
3. **Drain.** When producing stops, it waits for the stream and delayed set to empty.
4. **Analyze.** It reads the append-only results log (ADR-021). Each entry carries the
   job's enqueue and completion times from Redis `TIME`, so end-to-end latency and
   completion throughput come from the queue's own records, not the producers' clocks.
   Only the steady-state window [warmup, warmup + measure) counts. The log also gives
   the exactly-once check: every accepted job has exactly one result.

The report goes to `--out` (or stdout); progress goes to stderr.
"""

import argparse
import asyncio
import contextlib
import json
import logging
import os
import platform
import resource
import sys
import time
from collections import Counter
from concurrent.futures import ProcessPoolExecutor
from dataclasses import asdict, dataclass, field
from multiprocessing import get_context
from typing import Any

import redis.asyncio as aioredis
from redis.exceptions import ResponseError

from bench import analysis
from bench.analysis import Window
from ftq.client import Client, NewJob, QueueFull
from ftq.config import Settings, make_redis
from ftq.keys import Keys
from ftq.metrics import COUNTERS

log = logging.getLogger("bench.loadgen")

_SAMPLE_INTERVAL_S = 1.0
_ENQUEUE_BUCKET_MS = 0.1  # enqueue call histogram resolution
# Counters kept in the per-second samples (the rest are in the final snapshot).
_SAMPLED_COUNTERS = ("processed", "rejected", "blocked", "retried", "reclaimed", "dead")


@dataclass(frozen=True, slots=True)
class LoadSpec:
    """Everything that defines a run. Stored verbatim in the report."""

    redis_url: str
    queue: str = "bench"
    group: str = "workers"
    rate: float = 0.0  # total offered jobs/s across producers; 0 = back to back
    warmup: int = 10
    measure: int = 30
    cooldown: int = 5
    processes: int = 2
    batch: int = 500  # max jobs per enqueue_many call
    tick_ms: float = 10.0
    job_type: str = "send_email"
    payload: dict[str, Any] = field(default_factory=dict)
    payload_bytes: int = 100  # padded to this JSON size (if larger than the fields)
    max_depth: int = 0  # 0 = off; else producers hold off while depth >= this
    backpressure: str = "reject"
    high_watermark: int = 1_000_000
    low_watermark: int = 800_000
    block_timeout: float = 5.0
    expect_workers: int = 0  # wait for this many consumers before starting
    drain_timeout: float = 300.0

    @property
    def duration(self) -> int:
        return self.warmup + self.measure + self.cooldown

    def settings(self) -> Settings:
        return Settings(
            redis_url=self.redis_url,
            queue=self.queue,
            group=self.group,
            backpressure_mode="block" if self.backpressure == "block" else "reject",
            high_watermark=self.high_watermark,
            low_watermark=self.low_watermark,
            block_timeout=self.block_timeout,
            log_level="WARNING",
        )


def make_payload(spec: LoadSpec) -> dict[str, Any]:
    """spec.payload, padded with a `pad` string so its JSON encoding is payload_bytes
    long (handlers ignore fields they don't know). Never truncated: fields come first."""
    base = {"to": "user@example.com", **spec.payload, "pad": ""}
    size = len(json.dumps(base, separators=(",", ":")))
    base["pad"] = "x" * max(0, spec.payload_bytes - size)
    return base


def _cpu_seconds() -> float:
    usage = resource.getrusage(resource.RUSAGE_SELF)
    return usage.ru_utime + usage.ru_stime


# ---------------------------------------------------------------- producer processes


def _ready() -> int:
    """Pool warm-up: by the time this returns, the child has imported everything."""
    return os.getpid()


async def _next_tick(t0: float, tick: float) -> None:
    """Sleep until the next multiple of `tick` after t0 (a fixed grid, so sleeps that
    overshoot don't accumulate into a lower rate)."""
    elapsed = time.perf_counter() - t0
    await asyncio.sleep(tick - elapsed % tick)


def _producer_main(spec: LoadSpec, index: int, start_at: float) -> dict[str, Any]:
    return asyncio.run(_produce(spec, index, start_at))


async def _produce(spec: LoadSpec, index: int, start_at: float) -> dict[str, Any]:
    """One producer process: offer rate/processes jobs/s for spec.duration seconds."""
    settings = spec.settings()
    redis = make_redis(settings)
    client = Client(redis, settings)
    stream = Keys(spec.queue).stream
    payload = make_payload(spec)
    rate = spec.rate / spec.processes
    tick = spec.tick_ms / 1000
    total_due = int(rate * spec.duration)

    offered = 0
    per_second: dict[int, list[int]] = {}  # second -> [offered, accepted, rejected]
    call_ms: list[float] = []  # calls that started inside the window
    batch_sizes: Counter[int] = Counter()
    max_lag_s = 0.0
    depth_waits = 0
    try:
        await redis.ping()  # connect before the clock starts
        await asyncio.sleep(max(0.0, start_at - time.time()))
        start_late_s = time.time() - start_at
        t0 = time.perf_counter()
        cpu0 = _cpu_seconds()
        while (now := time.perf_counter() - t0) < spec.duration:
            if spec.max_depth and await redis.xlen(stream) >= spec.max_depth:
                depth_waits += 1
                await asyncio.sleep(tick)
                continue
            if rate > 0:
                due = min(int(rate * now), total_due) - offered
                if due < 1:
                    await _next_tick(t0, tick)
                    continue
                max_lag_s = max(max_lag_s, due / rate)
                n = min(due, spec.batch)
            else:
                n = spec.batch
            jobs = [NewJob(spec.job_type, payload) for _ in range(n)]
            started = time.perf_counter()
            try:
                accepted = len(await client.enqueue_many(jobs))
            except QueueFull as full:
                accepted = sum(1 for job_id in full.accepted if job_id is not None)
            elapsed_ms = (time.perf_counter() - started) * 1000
            offered += n
            bucket = per_second.setdefault(int(now), [0, 0, 0])
            bucket[0] += n
            bucket[1] += accepted
            bucket[2] += n - accepted
            if spec.warmup <= now < spec.warmup + spec.measure:
                call_ms.append(elapsed_ms)
                batch_sizes[n] += 1
            if rate > 0 and n == due:
                # Caught up: wait for the tick, so a batch is a tick's worth of jobs
                # rather than the one or two that came due during this call.
                await _next_tick(t0, tick)
        wall = time.perf_counter() - t0
        cpu = _cpu_seconds() - cpu0
    finally:
        await redis.aclose()
    c = client.counters
    return {
        "index": index,
        "pid": os.getpid(),
        "start_late_s": round(start_late_s, 4),
        "offered": offered,
        "accepted": c.accepted,
        "rejected": c.rejected,
        "blocked": c.blocked,
        "blocked_seconds": round(c.blocked_seconds, 3),
        "max_lag_s": round(max_lag_s, 4),
        "depth_waits": depth_waits,
        "cpu_busy": round(cpu / wall, 3),
        "per_second": per_second,
        "enqueue_call_hist_ms": analysis.histogram(call_ms, _ENQUEUE_BUCKET_MS),
        "batch_sizes": dict(batch_sizes),
    }


# ---------------------------------------------------------------- coordinator


async def _redis_ms(redis: aioredis.Redis) -> int:
    sec, usec = await redis.time()
    return int(sec) * 1000 + int(usec) // 1000


async def _wait_for_consumers(redis: aioredis.Redis, spec: LoadSpec) -> int:
    """Block until `expect_workers` consumers exist in the group (each worker registers
    on its first read), so the run doesn't start against a half-started fleet."""
    deadline = time.monotonic() + 120
    while True:
        try:
            consumers = len(await redis.xinfo_consumers(Keys(spec.queue).stream, spec.group))
        except ResponseError:  # no group yet: no worker has started
            consumers = 0
        if consumers >= spec.expect_workers:
            return consumers
        if time.monotonic() > deadline:
            raise RuntimeError(f"only {consumers} of {spec.expect_workers} workers after 120 s")
        await asyncio.sleep(0.25)


async def _sample(redis: aioredis.Redis, keys: Keys) -> dict[str, Any]:
    pipe = redis.pipeline(transaction=False)
    pipe.time()
    pipe.xlen(keys.stream)
    pipe.zcard(keys.delayed)
    pipe.hmget(keys.stats, list(_SAMPLED_COUNTERS))
    pipe.info("cpu")
    pipe.info("memory")
    (sec, usec), stream, delayed, counters, cpu, memory = await pipe.execute()
    return {
        "t_ms": int(sec) * 1000 + int(usec) // 1000,
        "depth": int(stream) + int(delayed),
        "stream": int(stream),
        "delayed": int(delayed),
        **{name: int(v or 0) for name, v in zip(_SAMPLED_COUNTERS, counters, strict=True)},
        # The main thread runs every command and script: its 1.0 is Redis's ceiling.
        "redis_cpu_main_s": cpu["used_cpu_user_main_thread"] + cpu["used_cpu_sys_main_thread"],
        "redis_cpu_s": cpu["used_cpu_user"] + cpu["used_cpu_sys"],
        "used_memory": int(memory["used_memory"]),
    }


async def _sampler(
    redis: aioredis.Redis, keys: Keys, out: list[dict[str, Any]], stop: asyncio.Event
) -> None:
    while not stop.is_set():
        out.append(await _sample(redis, keys))
        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(stop.wait(), _SAMPLE_INTERVAL_S)
    out.append(await _sample(redis, keys))


async def _environment(redis: aioredis.Redis) -> dict[str, Any]:
    server: Any = await redis.info("server")
    config: dict[str, Any] = {}
    for name in ("appendonly", "appendfsync", "maxmemory", "maxmemory-policy", "io-threads"):
        reply: Any = await redis.config_get(name)
        config.update(reply)
    return {
        "loadgen_host": platform.node(),
        "loadgen_platform": platform.platform(),
        "loadgen_cpus": os.cpu_count(),
        "python": platform.python_version(),
        "redis_version": server["redis_version"],
        "redis_config": config,
    }


def command_costs(
    before: dict[str, Any], after: dict[str, Any], jobs: int
) -> dict[str, dict[str, float]]:
    """What Redis spent per command over the run (`INFO commandstats` deltas), and per
    completed job. `usec` is time inside the command on the main thread. A script's
    `evalsha` time INCLUDES the commands it ran, which are also listed on their own
    (XADD inside commit.lua counts as xadd too), so the rows overlap: compare evalsha
    with the plain commands the clients sent (xreadgroup, xlen, ...), not the sum."""
    out: dict[str, dict[str, float]] = {}
    for name, stats in after.items():
        prev = before.get(name, {"calls": 0, "usec": 0})
        calls = int(stats["calls"]) - int(prev["calls"])
        usec = int(stats["usec"]) - int(prev["usec"])
        if calls <= 0:
            continue
        out[name.removeprefix("cmdstat_")] = {
            "calls": calls,
            "usec": usec,
            "calls_per_job": round(calls / jobs, 3) if jobs else 0.0,
            "usec_per_job": round(usec / jobs, 2) if jobs else 0.0,
        }
    return dict(sorted(out.items(), key=lambda kv: -kv[1]["usec"]))


async def _read_results(
    redis: aioredis.Redis, keys: Keys, after_id: str, window: Window
) -> tuple[list[tuple[int, int]], Counter[str], Counter[str]]:
    """Every results-log entry after `after_id`: (enqueued_at_ms, finished_at_ms) pairs,
    how many entries each job_id has (more than 1 = a duplicate result), and how many
    jobs each worker finished inside the window (for jobs per CPU-second per worker)."""
    times: list[tuple[int, int]] = []
    per_job: Counter[str] = Counter()
    by_worker: Counter[str] = Counter()
    cursor = after_id
    while True:
        reply: Any = await redis.xrange(keys.results, min=f"({cursor}", count=10_000)
        if not reply:
            return times, per_job, by_worker
        for entry_id, f in reply:
            finished = int(f["finished_at_ms"])
            times.append((int(f["enqueued_at_ms"]), finished))
            per_job[f["job_id"]] += 1
            if finished in window:
                by_worker[f["worker_id"]] += 1
            cursor = entry_id


async def run(spec: LoadSpec, meta: dict[str, Any]) -> dict[str, Any]:
    settings = spec.settings()
    keys = Keys(spec.queue)
    redis = make_redis(settings)
    try:
        env = await _environment(redis)
        consumers = await _wait_for_consumers(redis, spec)
        depth = await redis.xlen(keys.stream) + await redis.zcard(keys.delayed)
        if depth:
            raise RuntimeError(f"queue {spec.queue!r} isn't empty ({depth}): stale backlog")
        last: Any = await redis.xrevrange(keys.results, count=1)
        results_after = last[0][0] if last else "0-0"
        counters0: Any = await redis.hgetall(keys.stats)
        commands0: Any = await redis.info("commandstats")

        samples: list[dict[str, Any]] = []
        stop = asyncio.Event()
        loop = asyncio.get_running_loop()
        with ProcessPoolExecutor(spec.processes, mp_context=get_context("spawn")) as pool:
            await asyncio.gather(
                *(loop.run_in_executor(pool, _ready) for _ in range(spec.processes))
            )
            start_at = time.time() + 1.0
            producers = [
                loop.run_in_executor(pool, _producer_main, spec, i, start_at)
                for i in range(spec.processes)
            ]
            await asyncio.sleep(max(0.0, start_at - time.time()))
            t0_ms = await _redis_ms(redis)
            sampler = asyncio.create_task(_sampler(redis, keys, samples, stop))
            log.info("producing for %d s (%s)", spec.duration, meta.get("label", ""))
            produced = list(await asyncio.gather(*producers))

        drain_start = time.monotonic()
        drained = False
        while time.monotonic() - drain_start < spec.drain_timeout:
            if await redis.xlen(keys.stream) + await redis.zcard(keys.delayed) == 0:
                drained = True
                break
            await asyncio.sleep(0.2)
        drain_s = time.monotonic() - drain_start
        stop.set()
        await sampler
        log.info("drained=%s in %.1f s; reading the results log", drained, drain_s)

        window = Window.of_run(t0_ms, spec.warmup, spec.measure)
        times, per_job, by_worker = await _read_results(redis, keys, results_after, window)
        counters1: Any = await redis.hgetall(keys.stats)
        commands1: Any = await redis.info("commandstats")
        dlq = await redis.xlen(keys.dead)
    finally:
        await redis.aclose()

    return _report(
        spec, meta, env, consumers, window, t0_ms, samples, produced, times, per_job,
        by_worker, {k: int(counters1.get(k, 0)) - int(counters0.get(k, 0)) for k in COUNTERS},
        dlq, drained, drain_s,
        command_costs(commands0, commands1, len(per_job)),
    )  # fmt: skip


def _report(
    spec: LoadSpec,
    meta: dict[str, Any],
    env: dict[str, Any],
    consumers: int,
    window: Window,
    t0_ms: int,
    samples: list[dict[str, Any]],
    produced: list[dict[str, Any]],
    times: list[tuple[int, int]],
    per_job: Counter[str],
    by_worker: Counter[str],
    counters: dict[str, int],
    dlq: int,
    drained: bool,
    drain_s: float,
    commands: dict[str, dict[str, float]],
) -> dict[str, Any]:
    in_window = range(spec.warmup, spec.warmup + spec.measure)
    per_second: dict[int, list[int]] = {}
    for p in produced:
        for sec, (o, a, r) in p["per_second"].items():
            b = per_second.setdefault(int(sec), [0, 0, 0])
            b[0] += o
            b[1] += a
            b[2] += r
    offered_w = sum(v[0] for s, v in per_second.items() if s in in_window)
    accepted_w = sum(v[1] for s, v in per_second.items() if s in in_window)

    e2e = Counter(fin - enq for enq, fin in times if enq in window)
    completed_per_s = Counter((fin - t0_ms) // 1000 for _enq, fin in times)
    enqueue_hist = analysis.merge(p["enqueue_call_hist_ms"] for p in produced)
    batch_sizes = analysis.merge(p["batch_sizes"] for p in produced)

    accepted = sum(p["accepted"] for p in produced)
    duplicate_results = sum(n - 1 for n in per_job.values() if n > 1)
    missing = accepted - len(per_job)
    redis_main = [(s["t_ms"], s["redis_cpu_main_s"]) for s in samples]
    redis_all = [(s["t_ms"], s["redis_cpu_s"]) for s in samples]
    depth_w = [s["depth"] for s in samples if s["t_ms"] in window]

    return {
        "meta": meta,
        "spec": asdict(spec),
        "payload_json_bytes": len(json.dumps(make_payload(spec), separators=(",", ":"))),
        "env": env,
        "consumers": consumers,
        "t0_ms": t0_ms,
        "window": {"start_ms": window.start_ms, "end_ms": window.end_ms, "s": window.seconds},
        "throughput": {
            # The headline: jobs whose first commit landed inside the window, per second.
            "completed_per_s": round(analysis.rate_in(window, (f for _e, f in times)), 1),
            "offered_per_s": round(offered_w / spec.measure, 1),
            "accepted_per_s": round(accepted_w / spec.measure, 1),
        },
        # Jobs enqueued inside the window, enqueue -> first commit, Redis TIME (1 ms
        # resolution: the scripts stamp whole milliseconds).
        "e2e_latency_ms": analysis.summarize(e2e),
        "enqueue_call_ms": analysis.summarize(enqueue_hist),
        "enqueue_batch_jobs": analysis.summarize(batch_sizes),
        "depth_in_window": {
            "min": min(depth_w, default=0),
            "max": max(depth_w, default=0),
            "mean": round(sum(depth_w) / len(depth_w), 1) if depth_w else 0,
        },
        "redis": {
            "main_thread_busy": analysis.busy_fraction(redis_main, window),
            "all_threads_busy": analysis.busy_fraction(redis_all, window),
            "peak_used_memory": max((s["used_memory"] for s in samples), default=0),
        },
        "producers": {
            "offered": sum(p["offered"] for p in produced),
            "accepted": accepted,
            "rejected": sum(p["rejected"] for p in produced),
            "blocked": sum(p["blocked"] for p in produced),
            "blocked_seconds": round(sum(p["blocked_seconds"] for p in produced), 3),
            "cpu_busy": [p["cpu_busy"] for p in produced],
            "max_lag_s": max(p["max_lag_s"] for p in produced),
            "max_start_late_s": max(p["start_late_s"] for p in produced),
            "depth_waits": sum(p["depth_waits"] for p in produced),
        },
        "exactly_once": {
            "ok": drained and missing == 0 and duplicate_results == 0 and dlq == 0,
            "accepted": accepted,
            "results": sum(per_job.values()),
            "distinct_jobs_with_results": len(per_job),
            "missing": missing,
            "duplicate_results": duplicate_results,
            "dlq": dlq,
        },
        "drain": {"drained": drained, "s": round(drain_s, 2)},
        "counters": counters,
        "redis_commands": commands,
        # Jobs each worker (by worker_id: host-pid-suffix) finished inside the window.
        "completed_in_window_by_worker": dict(sorted(by_worker.items())),
        "histograms": {
            "e2e_latency_ms": {str(k): v for k, v in sorted(e2e.items())},
            "enqueue_call_ms": {str(k): v for k, v in enqueue_hist.items()},
        },
        "timeline": {
            "samples": samples,
            "producer_per_s": {str(k): v for k, v in sorted(per_second.items())},
            "completed_per_s": {str(k): v for k, v in sorted(completed_per_s.items())},
        },
    }


def _args(argv: list[str] | None) -> tuple[LoadSpec, dict[str, Any], str]:
    d = LoadSpec(redis_url="")
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawTextHelpFormatter)
    p.add_argument(
        "--redis-url", default=os.environ.get("FTQ_REDIS_URL", "redis://localhost:6379/0")
    )
    p.add_argument("--queue", default=d.queue)
    p.add_argument("--rate", type=float, default=d.rate, help="total jobs/s; 0 = back to back")
    p.add_argument("--warmup", type=int, default=d.warmup, help="seconds")
    p.add_argument("--measure", type=int, default=d.measure, help="seconds (the window)")
    p.add_argument("--cooldown", type=int, default=d.cooldown, help="seconds")
    p.add_argument("--processes", type=int, default=d.processes)
    p.add_argument("--batch", type=int, default=d.batch, help="max jobs per call")
    p.add_argument("--tick-ms", type=float, default=d.tick_ms)
    p.add_argument("--type", dest="job_type", default=d.job_type)
    p.add_argument("--payload", default="{}", help="JSON fields merged into each payload")
    p.add_argument("--payload-bytes", type=int, default=d.payload_bytes)
    p.add_argument("--max-depth", type=int, default=d.max_depth, help="saturation mode")
    p.add_argument("--backpressure", choices=["reject", "block"], default=d.backpressure)
    p.add_argument("--high-watermark", type=int, default=d.high_watermark)
    p.add_argument("--low-watermark", type=int, default=d.low_watermark)
    p.add_argument("--block-timeout", type=float, default=d.block_timeout)
    p.add_argument("--expect-workers", type=int, default=d.expect_workers)
    p.add_argument("--drain-timeout", type=float, default=d.drain_timeout)
    p.add_argument("--meta", default="{}", help="JSON copied into the report (labels)")
    p.add_argument("--out", default="-", help="report path, or - for stdout")
    a = p.parse_args(argv)
    spec = LoadSpec(
        redis_url=a.redis_url, queue=a.queue, rate=a.rate, warmup=a.warmup,
        measure=a.measure, cooldown=a.cooldown, processes=a.processes, batch=a.batch,
        tick_ms=a.tick_ms, job_type=a.job_type, payload=json.loads(a.payload),
        payload_bytes=a.payload_bytes, max_depth=a.max_depth, backpressure=a.backpressure,
        high_watermark=a.high_watermark, low_watermark=a.low_watermark,
        block_timeout=a.block_timeout, expect_workers=a.expect_workers,
        drain_timeout=a.drain_timeout,
    )  # fmt: skip
    return spec, json.loads(a.meta), a.out


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(
        level=logging.INFO, stream=sys.stderr, format="%(asctime)s loadgen %(message)s"
    )
    spec, meta, out = _args(argv)
    report = asyncio.run(run(spec, meta))
    text = json.dumps(report, indent=1) + "\n"
    if out == "-":
        sys.stdout.write(text)
    else:
        with open(out, "w") as f:
            f.write(text)
    t, lat = report["throughput"], report["e2e_latency_ms"]
    log.info(
        "completed %.0f/s (offered %.0f/s) | e2e p50 %s p99 %s ms | redis main %s | "
        "exactly-once %s",
        t["completed_per_s"],
        t["offered_per_s"],
        lat.get("p50"),
        lat.get("p99"),
        report["redis"]["main_thread_busy"],
        report["exactly_once"]["ok"],
    )
    return 0 if report["exactly_once"]["ok"] else 1


if __name__ == "__main__":
    sys.exit(main())
