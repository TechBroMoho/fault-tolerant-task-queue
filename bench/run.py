"""The local benchmark driver: run bench/loadgen.py against N worker containers.

    uv run python -m bench.run scaling            # 1, 2, 4, 8, 12 workers x 3 repeats
    uv run python -m bench.run concurrency        # one worker, FTQ_CONCURRENCY sweep
    uv run python -m bench.run latency            # e2e latency vs offered load
    uv run python -m bench.run backpressure       # offered load above capacity
    uv run python -m bench.run --help             # every knob

Topology (its own Compose project `ftq-bench`, so it never touches the dev stack's or
the chaos harness's Redis):

    loadgen (container) ──► redis ◄── worker-1..N (containers, `--scale`)

The load generator runs in a container on the same Docker network as Redis, the way it
will run as an ECS task beside Redis in Phase 8. That also keeps macOS's port-forwarding
path, which ADR-032 suspects of adding noise, out of every measured round trip.

Every point starts from scratch: `down -v`, a fresh Redis, N fresh workers, and the
loadgen waits until all N have registered. While the loadgen runs, this driver reads
every container's cgroup `cpu.stat` (exact CPU time, not `docker stats`' sampled
percentage), plus the Docker VM's `/proc/stat` for how busy the whole machine was. Each
reading is timestamped by the VM's own clock, which is also Redis's clock (one kernel),
so the CPU numbers line up with the loadgen's steady-state window without any
host-to-VM clock offset. The one report per point goes to `results/local/bench/<suite>/`.

These numbers are LOCAL: a laptop running Docker Desktop, where the workers, Redis, and
the load generator share one VM's CPUs. They validate the harness and show where this
machine's bottleneck is. They are not the project's headline throughput (Phase 8, AWS).
"""

import argparse
import asyncio
import contextlib
import json
import logging
import statistics
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from bench import analysis
from bench.analysis import Window
from chaos.topology import build_worker_image
from chaos.topology import run as sh

log = logging.getLogger("bench.run")

REPO_ROOT = Path(__file__).resolve().parents[1]
RESULTS = REPO_ROOT / "results" / "local" / "bench"
RUNS = REPO_ROOT / "bench" / "runs"  # generated compose file, logs (gitignored)
PROJECT = "ftq-bench"
IMAGE = "ftq-worker:local"
REDIS_IMAGE = "redis:8.8.3"  # the same pin as docker-compose.yml (ADR-004)
REDIS_PORT = 6391  # loopback only; not the dev stack's 6379 or chaos's 6390
ENVIRONMENT = "local / Docker Desktop (not the headline numbers; see Phase 8)"
_CPU_SAMPLE_S = 2.0


@dataclass(frozen=True, slots=True)
class Point:
    """One benchmark run: a worker fleet and the load offered to it."""

    suite: str
    label: str
    workers: int
    concurrency: int
    loadgen_args: tuple[str, ...]
    redis_io_threads: int = 1


def compose_spec(concurrency: int, io_threads: int = 1) -> dict[str, Any]:
    worker_env = {
        "FTQ_REDIS_URL": "redis://redis:6379/0",
        "FTQ_QUEUE": "bench",
        "FTQ_CONCURRENCY": str(concurrency),
        # Per-job lines are DEBUG; WARNING keeps even lifecycle lines out (ADR-033).
        "FTQ_LOG_LEVEL": "WARNING",
    }
    return {
        "name": PROJECT,
        "services": {
            "redis": {
                "image": REDIS_IMAGE,
                "container_name": f"{PROJECT}-redis",
                # The dev stack's durability and eviction settings (ADR-013): AOF
                # everysec, noeviction. A larger cap: a saturated 45 s run keeps every
                # done and ledger key of about a million jobs.
                "command": [
                    "redis-server",
                    "--appendonly",
                    "yes",
                    "--appendfsync",
                    "everysec",
                    "--maxmemory",
                    "4gb",
                    "--maxmemory-policy",
                    "noeviction",
                    # 1 = Redis's default: one thread does all command execution and
                    # socket I/O. More threads offload only the socket reads/writes.
                    "--io-threads",
                    str(io_threads),
                ],
                "ports": [f"127.0.0.1:{REDIS_PORT}:6379"],
                "healthcheck": {
                    "test": ["CMD", "redis-cli", "ping"],
                    "interval": "1s",
                    "timeout": "2s",
                    "retries": 30,
                },
            },
            "worker": {
                "image": IMAGE,
                "environment": worker_env,
                "restart": "no",
                "stop_grace_period": "20s",
                "depends_on": {"redis": {"condition": "service_healthy"}},
            },
            "loadgen": {
                "image": IMAGE,
                "working_dir": "/opt/ftq",
                "entrypoint": ["python", "-m", "bench.loadgen"],
                "environment": {"FTQ_REDIS_URL": "redis://redis:6379/0"},
                "profiles": ["loadgen"],
            },
        },
    }


class Stack:
    def __init__(self) -> None:
        self.file = RUNS / "compose.json"
        self._compose = ["docker", "compose", "-f", str(self.file), "-p", PROJECT]

    async def compose(self, *args: str, within: float = 300) -> str:
        return (await sh(*self._compose, *args, within=within)).out

    async def fresh(self, workers: int, concurrency: int, io_threads: int) -> None:
        """A fresh Redis (no data, no AOF) and `workers` new worker containers."""
        RUNS.mkdir(parents=True, exist_ok=True)
        self.file.write_text(json.dumps(compose_spec(concurrency, io_threads), indent=2) + "\n")
        await self.down()
        await self.compose("up", "-d", "--wait", "redis")
        await self.compose("up", "-d", "--no-deps", "--scale", f"worker={workers}", "worker")

    async def down(self) -> None:
        if self.file.exists():
            await self.compose("--profile", "loadgen", "down", "-v", "--remove-orphans")

    async def hostnames(self) -> dict[str, str]:
        """Container hostname (its short ID, which a worker_id starts with) -> name."""
        res = await sh(
            "docker", "ps", "--filter", f"label=com.docker.compose.project={PROJECT}",
            "--format", "{{.ID}} {{.Names}}",
        )  # fmt: skip
        return dict(line.split() for line in res.out.splitlines())

    async def containers(self) -> list[str]:
        res = await sh(
            "docker", "ps", "--filter", f"label=com.docker.compose.project={PROJECT}",
            "--format", "{{.Names}}",
        )  # fmt: skip
        return sorted(res.out.split())


# ---------------------------------------------------------------- CPU accounting


async def _read_cpu(name: str) -> tuple[str, int, float] | None:
    """(name, VM clock ms, the container's CPU seconds so far) from its cgroup (v2)."""
    res = await sh(
        "docker", "exec", name, "sh", "-c",
        "date +%s%N; grep usage_usec /sys/fs/cgroup/cpu.stat",
        check=False, within=30,
    )  # fmt: skip
    lines = res.out.split()
    if res.code != 0 or len(lines) < 3:
        return None  # the container is starting or gone; a missing sample is fine
    return name, int(lines[0]) // 1_000_000, int(lines[2]) / 1e6


async def _read_vm(redis: str) -> tuple[int, float, float] | None:
    """(VM clock ms, busy CPU seconds, total CPU seconds) of the whole Docker VM.
    /proc/stat isn't namespaced: read from any container, it's the VM's."""
    res = await sh(
        "docker", "exec", redis, "sh", "-c", "date +%s%N; head -1 /proc/stat",
        check=False, within=30,
    )  # fmt: skip
    parts = res.out.split()
    if res.code != 0 or len(parts) < 9:
        return None
    ticks = [int(x) for x in parts[2:]]  # user nice system idle iowait irq softirq steal
    idle = ticks[3] + ticks[4]
    total = sum(ticks[:8])
    return int(parts[0]) // 1_000_000, (total - idle) / 100, total / 100  # USER_HZ = 100


class CpuSampler:
    """Reads every container's cumulative CPU time every `_CPU_SAMPLE_S` seconds."""

    def __init__(self, stack: Stack) -> None:
        self._stack = stack
        self.per_container: dict[str, list[tuple[float, float]]] = {}
        self.vm: list[tuple[float, float, float]] = []
        self._stop = asyncio.Event()

    async def run(self) -> None:
        redis = f"{PROJECT}-redis"
        while True:
            names = await self._stack.containers()
            containers, vm = await asyncio.gather(
                asyncio.gather(*(_read_cpu(n) for n in names)), _read_vm(redis)
            )
            for r in containers:
                if r is not None:
                    name, t_ms, cpu_s = r
                    self.per_container.setdefault(name, []).append((t_ms, cpu_s))
            if vm is not None:
                self.vm.append(vm)
            if self._stop.is_set():
                return
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(self._stop.wait(), _CPU_SAMPLE_S)

    def stop(self) -> None:
        self._stop.set()

    def summary(self, window: Window) -> dict[str, Any]:
        """Each container's busy fraction (1.0 = one core) inside the window, by role."""
        busy = {
            name: analysis.busy_fraction(samples, window)
            for name, samples in sorted(self.per_container.items())
        }
        workers = [v for n, v in busy.items() if "-worker-" in n and v is not None]
        loadgen = [v for n, v in busy.items() if "-loadgen-" in n and v is not None]
        vm_busy = analysis.busy_fraction([(t, b) for t, b, _ in self.vm], window)
        vm_total = analysis.busy_fraction([(t, tot) for t, _b, tot in self.vm], window)
        return {
            "containers": busy,
            "workers_sum": round(sum(workers), 3),
            "worker_mean": round(statistics.mean(workers), 3) if workers else None,
            "worker_max": max(workers, default=None),
            "redis_container": busy.get(f"{PROJECT}-redis"),
            "loadgen_container": round(sum(loadgen), 3) if loadgen else None,
            # Busy CPUs of the whole VM, and how many it has (total CPU s per s).
            "vm_busy_cpus": vm_busy,
            "vm_cpus": round(vm_total) if vm_total else None,
            "samples": {"interval_s": _CPU_SAMPLE_S, "vm": len(self.vm)},
        }


# ---------------------------------------------------------------- one point


def _git_rev() -> str:
    def git(*args: str) -> str:
        return subprocess.run(
            ["git", *args], cwd=REPO_ROOT, capture_output=True, text=True, check=False
        ).stdout.strip()

    dirty = git("status", "--porcelain", "--untracked-files=no")
    rev = git("rev-parse", "--short", "HEAD")
    return f"{rev} (dirty: {', '.join(dirty.splitlines())})" if dirty else rev


async def run_point(stack: Stack, p: Point) -> dict[str, Any]:
    log.info("[%s] %s: %d workers, concurrency %d", p.suite, p.label, p.workers, p.concurrency)
    await stack.fresh(p.workers, p.concurrency, p.redis_io_threads)
    sampler = CpuSampler(stack)
    sampling = asyncio.create_task(sampler.run())
    meta = {
        "suite": p.suite,
        "label": p.label,
        "environment": ENVIRONMENT,
        "workers": p.workers,
        "worker_concurrency": p.concurrency,
        "redis_io_threads": p.redis_io_threads,
        "date_utc": datetime.now(UTC).isoformat(timespec="seconds"),
        "git": _git_rev(),
    }
    args = [
        "--profile", "loadgen", "run", "--rm", "-T", "--no-deps", "loadgen",
        "--expect-workers", str(p.workers), "--meta", json.dumps(meta), *p.loadgen_args,
    ]  # fmt: skip
    started = time.monotonic()
    try:
        res = await sh(*stack._compose, *args, within=1800, check=False)
    finally:
        sampler.stop()
        await sampling
    log_dir = RUNS / p.suite
    log_dir.mkdir(parents=True, exist_ok=True)
    (log_dir / f"{p.label}.loadgen.log").write_text(res.err)
    hosts = await stack.hostnames()
    worker_logs = await stack.compose("logs", "--no-color", "worker")
    (log_dir / f"{p.label}.workers.log").write_text(worker_logs)
    if res.code not in (0, 1):  # 1 = the run finished but exactly-once failed: keep it
        raise RuntimeError(f"loadgen exited {res.code}: {res.err[-2000:]}")
    report: dict[str, Any] = json.loads(res.out)
    window = Window(report["window"]["start_ms"], report["window"]["end_ms"])
    report["cpu"] = sampler.summary(window)
    report["per_worker"] = _per_worker(report, hosts, window)
    report["worker_log_lines"] = len(worker_logs.splitlines())
    report["meta"]["reproduce"] = _reproduce(p)
    report["meta"]["wall_s"] = round(time.monotonic() - started, 1)
    out = RESULTS / p.suite / f"{p.label}.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(report, indent=1) + "\n")
    _log_point(report)
    return report


def _per_worker(
    report: dict[str, Any], hosts: dict[str, str], window: Window
) -> dict[str, dict[str, float | None]]:
    """Each worker's jobs/s and jobs per CPU-second inside the window.

    If every worker does the same work per job, jobs per CPU-second is the speed of the
    core it ran on. A spread between workers of one run, or a fall as workers are
    added, is then the CPUs, not the queue (this machine mixes performance and
    efficiency cores; ADR-042)."""
    out: dict[str, dict[str, float | None]] = {}
    for worker_id, n in report["completed_in_window_by_worker"].items():
        name = hosts.get(worker_id.split("-", 1)[0], worker_id)
        busy = report["cpu"]["containers"].get(name)
        rate = n / window.seconds
        out[name] = {
            "completed_per_s": round(rate, 1),
            "cpu_busy": busy,
            "jobs_per_cpu_s": round(rate / busy, 1) if busy else None,
        }
    return out


def _reproduce(p: Point) -> str:
    return (
        f"uv run python -m bench.run point --suite {p.suite} --label {p.label} "
        f"--workers {p.workers} --concurrency {p.concurrency} --io-threads {p.redis_io_threads}"
        f" -- {' '.join(p.loadgen_args)}"
    )


def _log_point(r: dict[str, Any]) -> None:
    t, lat, cpu = r["throughput"], r["e2e_latency_ms"], r["cpu"]
    log.info(
        "  completed %.0f/s offered %.0f/s | e2e p50/p99 %s/%s ms | redis main %s | "
        "workers sum %s (max %s) | loadgen %s | vm %s of %s | exactly-once %s | depth %s",
        t["completed_per_s"], t["offered_per_s"], lat.get("p50"), lat.get("p99"),
        r["redis"]["main_thread_busy"], cpu["workers_sum"], cpu["worker_max"],
        cpu["loadgen_container"], cpu["vm_busy_cpus"], cpu["vm_cpus"],
        r["exactly_once"]["ok"], r["depth_in_window"],
    )  # fmt: skip


# ---------------------------------------------------------------- suites


def _timing(a: argparse.Namespace) -> list[str]:
    return [
        "--warmup", str(a.warmup), "--measure", str(a.measure), "--cooldown", str(a.cooldown),
        "--processes", str(a.processes), "--payload-bytes", str(a.payload_bytes),
    ]  # fmt: skip


def _saturate(a: argparse.Namespace) -> list[str]:
    """Saturation: back-to-back batches, held to a bounded backlog (ADR-041)."""
    return [*_timing(a), "--rate", "0", "--max-depth", str(a.max_depth), "--batch", "500"]


def capacity(suite: str, workers: int) -> float:
    """Median completed/s of the saved saturation runs at this worker count."""
    rates: list[float] = [
        r["throughput"]["completed_per_s"]
        for f in sorted((RESULTS / suite).glob(f"w{workers:02d}_*.json"))
        if (r := json.loads(f.read_text()))["meta"]["workers"] == workers
    ]
    if not rates:
        raise SystemExit(f"no {suite} results for {workers} workers: run that suite first")
    return statistics.median(rates)


def points(a: argparse.Namespace) -> list[Point]:
    if a.suite == "concurrency":
        # Repeat 1 is labelled c010 (the first sweep predates repeats), then c010_r2...
        return [
            Point("concurrency", f"c{c:03d}" + (f"_r{r}" if r > 1 else ""), 1, c,
                  tuple(_saturate(a)))
            for r in range(a.repeat_from, a.repeats + 1)
            for c in a.concurrency_values
        ]  # fmt: skip
    if a.suite == "iothreads":
        return [
            Point("iothreads", f"w{a.workers:02d}_io{a.io_threads}_r{r}", a.workers,
                  a.concurrency, tuple(_saturate(a)), a.io_threads)
            for r in range(a.repeat_from, a.repeats + 1)
        ]  # fmt: skip
    if a.suite == "scaling":
        return [
            Point("scaling", f"w{w:02d}_r{r}", w, a.concurrency, tuple(_saturate(a)))
            for r in range(a.repeat_from, a.repeats + 1)
            for w in a.worker_counts
        ]
    cap = capacity("scaling", a.workers)
    if a.suite == "latency":
        return [
            Point(
                "latency",
                f"w{a.workers:02d}_load{round(f * 100):03d}",
                a.workers,
                a.concurrency,
                (*_timing(a), "--rate", f"{cap * f:.0f}"),
            )
            for f in a.load_fractions
        ]
    if a.suite == "backpressure":
        rate = f"{cap * a.overload:.0f}"
        marks = ["--high-watermark", str(a.high), "--low-watermark", str(a.low)]
        return [
            Point(
                "backpressure",
                f"w{a.workers:02d}_{mode}",
                a.workers,
                a.concurrency,
                (*_timing(a), "--rate", rate, "--backpressure", mode, *marks),
            )
            for mode in ("reject", "block")
        ]
    raise SystemExit(f"unknown suite {a.suite}")


def _args(argv: list[str] | None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawTextHelpFormatter)
    p.add_argument(
        "suite",
        choices=["concurrency", "scaling", "latency", "backpressure", "iothreads", "point"],
    )
    p.add_argument("--skip-build", action="store_true", help="reuse ftq-worker:local as is")
    p.add_argument("--concurrency", type=int, default=50, help="FTQ_CONCURRENCY per worker")
    p.add_argument("--warmup", type=int, default=10)
    p.add_argument("--measure", type=int, default=30)
    p.add_argument("--cooldown", type=int, default=5)
    p.add_argument("--processes", type=int, default=2, help="loadgen producer processes")
    p.add_argument("--payload-bytes", type=int, default=100)
    p.add_argument("--max-depth", type=int, default=20_000, help="saturation backlog")
    p.add_argument("--concurrency-values", type=int, nargs="+", default=[10, 25, 50, 100])
    p.add_argument("--worker-counts", type=int, nargs="+", default=[1, 2, 4, 8, 12])
    p.add_argument("--repeats", type=int, default=3)
    p.add_argument("--repeat-from", type=int, default=1, help="add repeats to a saved suite")
    p.add_argument("--io-threads", type=int, default=1, help="Redis io-threads (iothreads, point)")
    p.add_argument("--workers", type=int, default=8, help="latency/backpressure fleet")
    p.add_argument("--load-fractions", type=float, nargs="+", default=[0.1, 0.25, 0.5, 0.75, 0.9])
    p.add_argument("--overload", type=float, default=1.5, help="backpressure: x capacity")
    p.add_argument("--high", type=int, default=20_000, help="backpressure high watermark")
    p.add_argument("--low", type=int, default=15_000, help="backpressure low watermark")
    # `point`: one run with explicit loadgen args after `--`.
    p.add_argument("--label", default="point")
    p.add_argument("--point-suite", "--suite", dest="point_suite", default="adhoc")
    argv = sys.argv[1:] if argv is None else argv
    split = argv.index("--") if "--" in argv else len(argv)
    a = p.parse_args(argv[:split])
    a.loadgen_args = argv[split + 1 :]
    return a


async def main_async(a: argparse.Namespace) -> int:
    if a.suite == "point":
        todo = [
            Point(a.point_suite, a.label, a.workers, a.concurrency, tuple(a.loadgen_args),
                  a.io_threads)
        ]  # fmt: skip
    else:
        todo = points(a)
    if not a.skip_build:
        log.info("building %s", IMAGE)
        await build_worker_image(REPO_ROOT)
    stack = Stack()
    failures = 0
    try:
        for i, p in enumerate(todo, 1):
            log.info("point %d of %d", i, len(todo))
            report = await run_point(stack, p)
            failures += not report["exactly_once"]["ok"]
    finally:
        await stack.down()
    log.info("done: %d points, %d exactly-once failures", len(todo), failures)
    return 1 if failures else 0


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(
        level=logging.INFO, stream=sys.stdout, format="%(asctime)s %(levelname)s %(message)s"
    )
    return asyncio.run(main_async(_args(argv)))


if __name__ == "__main__":
    sys.exit(main())
