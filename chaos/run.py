"""Chaos orchestrator: `uv run python -m chaos.run --jobs N [--workers W] [--seed S]`.

1. Start a fresh generated topology (Redis, Toxiproxy with one proxy per worker, W
   worker containers built from the working tree).
2. Enqueue N jobs of the mix (mix.py) at a steady rate, directly into Redis, while the
   seeded fault schedule kills, pauses, and partitions workers and a supervisor restarts
   the ones that crash.
3. Heal everything, wait for the queue to drain (with a global timeout), stop the
   workers gracefully, and run the verifier (I1-I5).
4. Write the JSON report (default `results/local/chaos_report.json`), plus the worker
   logs and the accepted-job list in a run directory, and exit non-zero on any violation.

The seed is printed first and recorded in the report: the same seed reproduces the same
job mix and the same fault *plan*. (Timing still varies, so the exact interleaving, and
therefore counts like reclaims, differ between runs.) Long runs outlive a 10-minute
shell limit: run it in the background with its output in a log file and poll the file.
"""

import argparse
import asyncio
import json
import logging
import random
import secrets
import subprocess
import sys
import time
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import redis.asyncio as aioredis

from chaos import faults, mix, topology
from chaos.toxiproxy import Toxiproxy
from chaos.verifier import Minimums, verify
from ftq.client import Client
from ftq.config import Settings, make_redis
from ftq.keys import Keys
from ftq.metrics import snapshot

log = logging.getLogger("chaos")
REPO_ROOT = Path(__file__).resolve().parents[1]
QUEUE = "chaos"
BATCH = 100  # jobs per enqueue_many call

# Worker settings for chaos runs. Short leases so reclaims happen within the run; retry
# backoff short so flaky jobs finish; no TTLs so the verifier can read every key
# (ADR-010). Sizing of max_attempts / max_deliveries: ADR-008.
LEASE = 2.0
WORKER_ENV = {
    "FTQ_QUEUE": QUEUE,
    "FTQ_BLOCK_MS": "500",
    "FTQ_SOCKET_TIMEOUT": "2",
    "FTQ_SOCKET_CONNECT_TIMEOUT": "1",
    "FTQ_VISIBILITY_TIMEOUT": str(LEASE),
    "FTQ_HEARTBEAT_INTERVAL": "0.5",
    "FTQ_REAP_INTERVAL": "0.5",
    "FTQ_MAX_ATTEMPTS": "8",
    "FTQ_MAX_DELIVERIES": "12",
    "FTQ_JOB_BACKOFF_BASE": "0.1",
    "FTQ_JOB_BACKOFF_CAP": "2",
    "FTQ_JOB_TIMEOUT": "10",
    "FTQ_SHUTDOWN_GRACE": "10",
    "FTQ_DONE_TTL_SECONDS": "0",
    # Restarts leave consumer records behind; prune them during the run, which exercises
    # the "never delete a consumer that owns entries" rule under chaos (ADR-029).
    "FTQ_CONSUMER_PRUNE_IDLE": "10",
    "FTQ_CONSUMER_PRUNE_INTERVAL": "5",
    "FTQ_LOG_LEVEL": "INFO",
    "FTQ_LOG_FORMAT": "json",
}


def _args(argv: list[str] | None) -> argparse.Namespace:
    p = argparse.ArgumentParser(prog="chaos.run", description=__doc__.split("\n\n")[0])
    p.add_argument("--jobs", type=int, required=True)
    p.add_argument("--workers", type=int, default=8)
    p.add_argument("--concurrency", type=int, default=16, help="FTQ_CONCURRENCY per worker")
    p.add_argument("--seed", type=int, default=None, help="default: random (printed)")
    p.add_argument("--rate", type=float, default=2000, help="enqueue rate, jobs/s")
    p.add_argument(
        "--fault-tail", type=float, default=20, help="seconds of faults after the last enqueue"
    )
    p.add_argument("--drain-timeout", type=float, default=600)
    p.add_argument("--out", type=Path, default=REPO_ROOT / "results/local/chaos_report.json")
    p.add_argument(
        "--run-dir",
        type=Path,
        default=None,
        help="logs + accepted jobs (default chaos/runs/<utc time>)",
    )
    p.add_argument("--keep", action="store_true", help="leave the stack running afterwards")
    p.add_argument("--skip-build", action="store_true", help="reuse ftq-worker:local as is")
    return p.parse_args(argv)


def _git_rev() -> str:
    """HEAD's short hash, with "-dirty(<paths>)" if tracked files differ from it, so a
    report says exactly which code it ran."""
    try:
        rev = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"],
            capture_output=True,
            text=True,
            cwd=REPO_ROOT,
            check=True,
        ).stdout.strip()
        dirty = subprocess.run(
            ["git", "status", "--porcelain", "--untracked-files=no"],
            capture_output=True,
            text=True,
            cwd=REPO_ROOT,
            check=True,
        ).stdout.splitlines()
        paths = ",".join(line[3:] for line in dirty)
        return rev + (f"-dirty({paths})" if dirty else "")
    except (OSError, subprocess.CalledProcessError):
        return "unknown"


class Run:
    def __init__(self, a: argparse.Namespace) -> None:
        self.a = a
        self.seed = a.seed if a.seed is not None else secrets.randbelow(2**31)
        self.env = {**WORKER_ENV, "FTQ_CONCURRENCY": str(a.concurrency)}
        self.max_attempts = int(self.env["FTQ_MAX_ATTEMPTS"])
        self.max_deliveries = int(self.env["FTQ_MAX_DELIVERIES"])
        self.run_dir: Path = (
            a.run_dir or REPO_ROOT / "chaos/runs" / datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
        ).resolve()
        self.stack = topology.Stack(self.run_dir / "compose.json")
        self.settings = Settings(
            redis_url=f"redis://127.0.0.1:{topology.REDIS_PORT}/0",
            queue=QUEUE,
            # The producer waits for room instead of failing if the queue backs up.
            backpressure_mode="block",
            block_timeout=a.drain_timeout,
            done_ttl_seconds=0,
        )
        self.keys = Keys(QUEUE)
        self.accepted: dict[str, str] = {}  # job_id -> kind
        self.enqueue_done = False
        self.cpu_samples: list[dict[str, float]] = []

    # ------------------------------------------------------------ phases

    async def main(self) -> int:
        a = self.a
        rng = random.Random(self.seed)
        params = mix.MixParams(LEASE, self.max_attempts, self.max_deliveries)
        jobs = mix.build(a.jobs, rng, params)
        enqueue_s = a.jobs / a.rate
        span = enqueue_s + a.fault_tail
        plan = faults.plan(rng, a.workers, LEASE, span)
        span = max(span, max(f.at + f.duration for f in plan))
        log.info(
            "seed %d | %d jobs | %d workers | %s",
            self.seed,
            a.jobs,
            a.workers,
            dict(mix.counts(a.jobs)),
        )
        log.info(
            "plan: %d faults over %.0fs (%s)", len(plan), span, dict(Counter(f.kind for f in plan))
        )

        topology.write_compose(self.stack_file, a.workers, self.env, self.seed)
        if not a.skip_build:
            log.info("building %s", topology.WORKER_IMAGE)
            await topology.build_worker_image(REPO_ROOT)
        await self.stack.down()
        await self.stack.compose("up", "-d", "--wait", "redis", "toxiproxy")
        toxi = Toxiproxy(f"http://127.0.0.1:{topology.TOXIPROXY_API_PORT}")
        redis = make_redis(self.settings)
        started = time.monotonic()
        try:
            await self._wait_toxiproxy(toxi)
            for i in range(1, a.workers + 1):
                await toxi.create_proxy(
                    topology.proxy_name(i), f"0.0.0.0:{topology.PROXY_BASE_PORT + i}", "redis:6379"
                )
            workers = [f"worker-{i}" for i in range(1, a.workers + 1)]
            await self.stack.compose("up", "-d", *workers)
            await self._wait_consumers(redis, a.workers)

            injector = faults.Injector(self.stack, toxi)
            supervisor = faults.Supervisor(self.stack, injector)
            stop_supervisor = asyncio.Event()
            sup_task = asyncio.create_task(supervisor.run(stop_supervisor))
            progress = asyncio.create_task(self._progress(redis, injector, supervisor))
            t0 = asyncio.get_running_loop().time()
            producer = asyncio.create_task(self._produce(redis, jobs, t0))
            await asyncio.gather(producer, injector.run(plan, t0))
            fault_phase_s = asyncio.get_running_loop().time() - t0
            log.info("fault phase over after %.1fs; healing everything", fault_phase_s)
            await faults.heal_all(self.stack, toxi)

            drain_started = time.monotonic()
            drained = await self._wait_drained(redis)
            drain_s = time.monotonic() - drain_started
            stop_supervisor.set()
            await sup_task
            progress.cancel()
            log.info("stopping workers (SIGTERM, graceful drain)")
            await self.stack.compose("stop", *workers, within=180)
            logs = await self._collect_logs(a.workers)

            evidence = {
                **{k: v for k, v in injector.counts().items() if not k.startswith("by_")},
                "pool_resets": logs["pool_resets"],
                "crash_restarts": supervisor.restarts.get(70, 0),
                "unexpected_exits": sum(n for code, n in supervisor.restarts.items() if code != 70),
                # Workers log JSON only; a non-JSON line is a traceback or a crash message.
                "error_log_lines": logs["by_level"].get("ERROR", 0)
                + logs["by_level"].get("non-json", 0),
            }
            result = await verify(
                redis,
                self.keys,
                self.settings.group,
                self.accepted,
                self.max_attempts,
                self.max_deliveries,
                evidence,
                Minimums(),
            )
            # Measured, not estimated: every done key, hash, and log entry is still there
            # (no TTLs in chaos runs), so this is the whole run's footprint against the
            # noeviction cap. The 1M run is what sizes it (ADR-040).
            mem = await redis.info("memory")
            redis_memory = {k: mem[k] for k in ("used_memory", "used_memory_peak", "maxmemory")}
            report = self._report(
                result,
                plan,
                injector,
                supervisor,
                logs,
                drained,
                fault_phase_s,
                drain_s,
                time.monotonic() - started,
                redis_memory,
            )
        finally:
            await redis.aclose()
            await toxi.aclose()
            if not a.keep:
                await self.stack.down()

        a.out.parent.mkdir(parents=True, exist_ok=True)
        a.out.write_text(json.dumps(report, indent=2) + "\n")
        self._summary(report)
        return 0 if report["passed"] else 1

    @property
    def stack_file(self) -> Path:
        return self.run_dir / "compose.json"

    async def _wait_toxiproxy(self, toxi: Toxiproxy) -> None:
        for _ in range(100):
            try:
                await toxi.ping()
                return
            except Exception:
                await asyncio.sleep(0.2)
        raise RuntimeError("Toxiproxy API never came up")

    async def _wait_consumers(self, redis: aioredis.Redis, n: int) -> None:
        """Wait until every worker has joined the group (each one's first XREADGROUP)."""
        for _ in range(300):
            snap = await snapshot(redis, self.settings)
            if snap["consumers"] >= n:
                return
            await asyncio.sleep(0.1)
        raise RuntimeError(f"only {snap['consumers']} of {n} workers joined the group")

    async def _produce(self, redis: aioredis.Redis, jobs: list[tuple[str, Any]], t0: float) -> None:
        """Enqueue in batches at a steady rate. A job counts as accepted only once its
        enqueue returned a job_id (SPEC §4: accepted = the call returned)."""
        client = Client(redis, self.settings)
        loop = asyncio.get_running_loop()
        with (self.run_dir / "accepted.jsonl").open("w") as f:
            for i in range(0, len(jobs), BATCH):
                await asyncio.sleep(max(0.0, t0 + i / self.a.rate - loop.time()))
                batch = jobs[i : i + BATCH]
                ids = await client.enqueue_many([job for _kind, job in batch])
                for job_id, (kind, _job) in zip(ids, batch, strict=True):
                    self.accepted[job_id] = kind
                    f.write(json.dumps({"job_id": job_id, "kind": kind}) + "\n")
        self.enqueue_done = True
        log.info(
            "producer: %d jobs accepted in %.1fs (blocked %d)",
            len(self.accepted),
            loop.time() - t0,
            client.counters.blocked,
        )

    async def _progress(
        self, redis: aioredis.Redis, injector: faults.Injector, supervisor: faults.Supervisor
    ) -> None:
        n = 0
        while True:
            await asyncio.sleep(5)
            n += 1
            try:
                s = await snapshot(redis, self.settings)
                c = s["counters"]
                log.info(
                    "progress: accepted %d/%d processed %d dlq %d | depth %d pel %d delayed %d"
                    " | faults %d restarts %d | reclaimed %d dup_suppressed %d timeouts %d",
                    len(self.accepted),
                    self.a.jobs,
                    c["processed"],
                    s["dlq"],
                    s["depth"],
                    s["in_flight"],
                    s["delayed"],
                    len(injector.executed),
                    sum(supervisor.restarts.values()),
                    c["reclaimed"],
                    c["duplicates_suppressed"],
                    c["timeouts"],
                )
                if n % 3 == 0:
                    await self._sample_cpu()
            except Exception as exc:  # progress is best effort; never kill the run
                log.warning("progress: %r", exc)

    async def _sample_cpu(self) -> None:
        res = await topology.run(
            "docker",
            "stats",
            "--no-stream",
            "--format",
            "{{.Name}}\t{{.CPUPerc}}\t{{.MemUsage}}",
            check=False,
        )
        sample = {}
        for line in res.out.splitlines():
            name, cpu, _mem = line.split("\t")
            if name.startswith(topology.PROJECT):
                sample[name] = float(cpu.rstrip("%") or 0)
        self.cpu_samples.append(sample)

    async def _wait_drained(self, redis: aioredis.Redis) -> bool:
        """Drained = stream, PEL, and delayed set all empty on 3 polls in a row, a second
        apart. Returns False if the drain timeout ran out first."""
        deadline = time.monotonic() + self.a.drain_timeout
        quiet = 0
        while time.monotonic() < deadline:
            s = await snapshot(redis, self.settings)
            quiet = quiet + 1 if s["depth"] == 0 and s["in_flight"] == 0 else 0
            if quiet >= 3:
                return True
            await asyncio.sleep(1)
        log.error("drain timeout (%.0fs) ran out", self.a.drain_timeout)
        return False

    async def _collect_logs(self, workers: int) -> dict[str, Any]:
        """Save each worker's logs and count the lines that are evidence (pool resets,
        orphans, timeouts) or trouble (ERROR)."""
        levels: Counter[str] = Counter()
        messages: Counter[str] = Counter()
        errors: list[str] = []
        pool_start_s: list[float] = []
        # Timeouts of runs that can't hang (the mix's hang jobs hang only on their first
        # attempt or two): each one is a healthy run the timeout cut short. Evidence, not
        # an invariant; on a starved machine it's the pool-reset cascade (ADR-039).
        false_timeouts: Counter[str] = Counter()
        for i in range(1, workers + 1):
            name = topology.worker_name(i)
            res = await topology.run("docker", "logs", name, check=False)
            text = res.out + res.err
            (self.run_dir / f"{name}.log").write_text(text)
            for line in text.splitlines():
                try:
                    rec = json.loads(line)
                except json.JSONDecodeError:
                    levels["non-json"] += 1
                    continue
                levels[rec.get("level", "?")] += 1
                msg = str(rec.get("msg", ""))
                for marker in (
                    "process pool reset",
                    "orphaned thread run finished",
                    "exceeded its",
                    "lost the lease",
                    ": started (",
                    "pruned",
                    "job -> DLQ",
                    "succeeded after it was moved",
                ):
                    if marker in msg:
                        messages[marker] += 1
                if msg.startswith("process pool ready:"):
                    pool_start_s.append(float(msg.rsplit(" in ", 1)[1].rstrip("s")))
                if msg.startswith("HandlerTimeout") and not _may_hang(
                    self.accepted.get(str(rec.get("job_id")), "?"), int(rec.get("attempt", 0))
                ):
                    false_timeouts[self.accepted.get(str(rec.get("job_id")), "?")] += 1
                if rec.get("level") == "ERROR" and len(errors) < 20:
                    errors.append(line[:500])
        return {
            "by_level": dict(levels),
            "by_message": dict(messages),
            "pool_resets": messages["process pool reset"],
            "pool_starts": {
                "count": len(pool_start_s),
                "max_s": max(pool_start_s, default=0.0),
                "mean_s": round(sum(pool_start_s) / len(pool_start_s), 3) if pool_start_s else 0.0,
            },
            "timeouts_of_runs_that_cannot_hang": dict(false_timeouts),
            "error_samples": errors,
        }

    def _report(
        self,
        result: dict[str, Any],
        plan: list[faults.Fault],
        injector: faults.Injector,
        supervisor: faults.Supervisor,
        logs: dict[str, Any],
        drained: bool,
        fault_phase_s: float,
        drain_s: float,
        total_s: float,
        redis_memory: dict[str, int],
    ) -> dict[str, Any]:
        a = self.a
        cpu: dict[str, list[float]] = {}
        for sample in self.cpu_samples:
            for name, pct in sample.items():
                cpu.setdefault(name, []).append(pct)
        return {
            "passed": result["passed"],
            "run": {
                "date_utc": datetime.now(UTC).isoformat(timespec="seconds"),
                "git": _git_rev(),
                "reproduce": (
                    f"uv run python -m chaos.run --jobs {a.jobs} --workers {a.workers}"
                    f" --concurrency {a.concurrency} --seed {self.seed} --rate {a.rate:g}"
                    f" --fault-tail {a.fault_tail:g}"
                ),
                "seed": self.seed,
                "jobs": a.jobs,
                "accepted": len(self.accepted),
                "workers": a.workers,
                "mix": mix.counts(a.jobs),
                "worker_env": {**self.env, "FTQ_REDIS_URL": "redis://toxiproxy:2000<i>/0"},
                "docker": {
                    "cpus": _docker_info("{{.NCPU}}"),
                    "mem_bytes": _docker_info("{{.MemTotal}}"),
                },
                "seconds": {
                    "fault_phase": round(fault_phase_s, 1),
                    "drain": round(drain_s, 1),
                    "total": round(total_s, 1),
                },
                "drained_before_timeout": drained,
                "redis_memory_bytes": redis_memory,
            },
            "verifier": result,
            "faults": {
                "planned": len(plan),
                "executed": injector.counts(),
                "crash_restarts_by_exit_code": {str(k): v for k, v in supervisor.restarts.items()},
                "timeline": injector.executed,
                "skipped": injector.skipped,
            },
            "worker_logs": logs,
            "cpu_percent_mean": {k: round(sum(v) / len(v), 1) for k, v in sorted(cpu.items())},
            # Relative when inside the repo (the default); a --run-dir elsewhere is kept
            # absolute rather than crashing after the run and losing the report.
            "run_dir": str(
                self.run_dir.relative_to(REPO_ROOT)
                if self.run_dir.is_relative_to(REPO_ROOT)
                else self.run_dir
            ),
        }

    def _summary(self, report: dict[str, Any]) -> None:
        v = report["verifier"]
        log.info(
            "==== chaos run %s (seed %d, %d jobs, %d workers) ====",
            "PASSED" if report["passed"] else "FAILED",
            self.seed,
            self.a.jobs,
            self.a.workers,
        )
        for name, inv in v["invariants"].items():
            log.info(
                "  %-26s %s %s",
                name,
                "ok  " if inv["ok"] else "FAIL",
                "; ".join(inv["violations"][:3]),
            )
        c = v["counters"]
        log.info(
            "  faults %s | crash restarts %s",
            report["faults"]["executed"],
            report["faults"]["crash_restarts_by_exit_code"],
        )
        log.info(
            "  processed %d dead %d reclaimed %d duplicates_suppressed %d "
            "effects_suppressed %d timeouts %d lease_lost %d",
            c["processed"],
            c["dead"],
            c["reclaimed"],
            c["duplicates_suppressed"],
            c["effects_suppressed"],
            c["timeouts"],
            c["lease_lost"],
        )
        log.info("  reclaims by delivery count: %s", v["reclaims_by_delivery"])
        log.info(
            "  ... excluding crashy jobs: %s (max %d, max_deliveries %d)",
            v["reclaims_by_delivery_excluding_crashy"],
            v["max_delivery_excluding_crashy"],
            self.max_deliveries,
        )
        wl = report["worker_logs"]
        log.info(
            "  pool resets %d | pool starts %s | timeouts of runs that can't hang %s",
            wl["pool_resets"],
            wl["pool_starts"],
            wl["timeouts_of_runs_that_cannot_hang"],
        )
        r = report["run"]
        mem = r["redis_memory_bytes"]
        log.info(
            "  seconds %s | redis memory peak %.0f MiB of maxmemory %.0f MiB",
            r["seconds"],
            mem["used_memory_peak"] / 2**20,
            mem["maxmemory"] / 2**20,
        )
        log.info("  report: %s", self.a.out)


def _may_hang(kind: str, attempt: int) -> bool:
    """Whether a run of this mix kind at this attempt can hang (chaos/mix.py): `hang` and
    `hang_process` on attempts < hang_attempts <= 2, `hang_thread` on attempt 0, and
    `hang_forever` always."""
    limit = {"hang": 2, "hang_process": 2, "hang_thread": 1}.get(kind, 0)
    return kind == "hang_forever" or attempt < limit


def _docker_info(fmt: str) -> str:
    try:
        return subprocess.run(
            ["docker", "info", "--format", fmt], capture_output=True, text=True, check=True
        ).stdout.strip()
    except (OSError, subprocess.CalledProcessError):
        return "unknown"


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(
        level=logging.INFO, stream=sys.stdout, format="%(asctime)s %(levelname)s %(message)s"
    )
    logging.getLogger("httpx").setLevel(logging.WARNING)  # one line per Toxiproxy call
    a = _args(argv)
    run = Run(a)
    run.run_dir.mkdir(parents=True, exist_ok=True)
    print(f"chaos seed: {run.seed}", flush=True)
    try:
        return asyncio.run(run.main())
    except Exception:
        log.exception("chaos run aborted (harness error, not a verdict)")
        return 2


if __name__ == "__main__":
    sys.exit(main())
