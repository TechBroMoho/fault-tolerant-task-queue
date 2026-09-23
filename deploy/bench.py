"""The Phase 8 benchmark driver (`make aws-bench`): run the session's points on AWS.

    python -m deploy.bench plan                  # the points, their time and cost ($0)
    python -m deploy.bench run                   # BILLABLE: the stack must be up
    python -m deploy.bench run --backend local   # the same driver against local Docker

Each point, in order (ADR-048):
1. scale the worker service to 0, and FLUSHALL (every point starts from an empty Redis,
   so memory, the results log, and the consumer group never carry over);
2. scale the workers to N and wait until N are running;
3. save a snapshot of the running worker tasks (the "12 workers ran" evidence, SPEC §9);
4. start the loadgen pair: a producer-only task on one loadgen host, the coordinator on
   the other (ADR-047), each pinned to its own instance;
5. wait for both, decode the coordinator's report from its log, and save it with the
   producer host's log under results/aws/<suite>/.

The laptop only starts tasks and polls (SPEC §7). Every loadgen task stops itself
(`timeout`, task_max_seconds), and the driver stops starting points at its deadline.
Points whose results already exist are skipped, so an interrupted session can resume.
The local backend runs the same steps on its own Compose project (port 6392), never on
the dev Redis.
"""

import argparse
import base64
import gzip
import json
import logging
import shlex
import statistics
import subprocess
import time
import uuid
from collections.abc import Callable, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Protocol

log = logging.getLogger("deploy.bench")

REPO_ROOT = Path(__file__).resolve().parents[1]
RESULTS = REPO_ROOT / "results" / "aws"
QUEUE = "bench"
MAX_DEPTH = 20_000  # saturation backlog (ADR-041), shared by both hosts' producers
# Measured in Phase 7: scale to 0, flush, scale up, start 2 tasks, fetch logs.
POINT_OVERHEAD_S = 90
# The whole-session costs outside the points: apply + boot + register, image push,
# teardown and verify-clean.
SESSION_OVERHEAD_S = 15 * 60
ENVIRONMENT = (
    "AWS us-west-2a, ECS on EC2: Redis on m7i-flex.large, 2 loadgen hosts and the "
    "workers on c7i-flex.large (2 workers per host)"
)


# ---------------------------------------------------------------- the session (pure)


@dataclass(frozen=True, slots=True)
class BenchPoint:
    suite: str
    label: str
    workers: int
    warmup: int
    measure: int
    cooldown: int
    args: tuple[str, ...]  # loadgen flags besides timing, hosts, and the run id
    # Backpressure points offer a multiple of the headline capacity, known only once the
    # headline runs are saved: resolved at run time.
    overload: float = 0.0

    @property
    def seconds(self) -> int:
        """Planning estimate: the run, its drain, and the per-point overhead."""
        return self.warmup + self.measure + self.cooldown + POINT_OVERHEAD_S


@dataclass(frozen=True, slots=True)
class SessionSpec:
    worker_counts: tuple[int, ...] = (1, 2, 4, 8, 12)
    scaling_measure: int = 180  # SPEC: ~3 min steady state per worker count
    headline_workers: int = 12
    headline_measure: int = 300  # SPEC: >= 5 min
    headline_repeats: int = 3
    backpressure_measure: int = 120
    overload: float = 1.5
    high_watermark: int = 200_000
    low_watermark: int = 150_000
    warmup: int = 20
    cooldown: int = 5
    processes: int = 4  # per loadgen host (Phase 6: >= 4 above ~20K/s)
    suites: tuple[str, ...] = ("scaling", "headline", "backpressure")


def session_points(s: SessionSpec) -> list[BenchPoint]:
    saturate = ("--rate", "0", "--max-depth", str(MAX_DEPTH), "--batch", "500")
    procs = ("--processes", str(s.processes))
    points: list[BenchPoint] = []
    if "scaling" in s.suites:
        points += [
            BenchPoint("scaling", f"w{w:02d}", w, s.warmup, s.scaling_measure, s.cooldown,
                       (*procs, *saturate))
            for w in s.worker_counts
        ]  # fmt: skip
    if "headline" in s.suites:
        points += [
            BenchPoint("headline", f"w{s.headline_workers:02d}_r{r}", s.headline_workers,
                       s.warmup, s.headline_measure, s.cooldown, (*procs, *saturate))
            for r in range(1, s.headline_repeats + 1)
        ]  # fmt: skip
    if "backpressure" in s.suites:
        marks = ("--high-watermark", str(s.high_watermark),
                 "--low-watermark", str(s.low_watermark))  # fmt: skip
        points += [
            BenchPoint("backpressure", f"w{s.headline_workers:02d}_{mode}",
                       s.headline_workers, s.warmup, s.backpressure_measure, s.cooldown,
                       (*procs, "--backpressure", mode, *marks), overload=s.overload)
            for mode in ("reject", "block")
        ]  # fmt: skip
    return points


def session_seconds(points: Sequence[BenchPoint]) -> int:
    return sum(p.seconds for p in points) + SESSION_OVERHEAD_S


def backpressure_rate(headline_rates: Sequence[float], overload: float) -> float:
    """Offered load for a backpressure point: `overload` x the median headline capacity."""
    if not headline_rates:
        raise ValueError("no headline results yet: run the headline suite first")
    return overload * statistics.median(headline_rates)


# ---------------------------------------------------------------- commands (pure)

# Written into the coordinator's log after the run: the whole raw report, gzip + base64,
# in chunks well under CloudWatch's 256 KB event limit.
_DUMP = (
    "import base64,gzip,os,textwrap\n"
    "p='/tmp/report.json'\n"
    "if os.path.exists(p):\n"
    " d=base64.b64encode(gzip.compress(open(p,'rb').read())).decode()\n"
    " print('REPORT-BEGIN',flush=True)\n"
    " [print('R:'+c,flush=True) for c in textwrap.wrap(d,60000)]\n"
    " print('REPORT-END',flush=True)\n"
)
FLUSH_COMMAND = [
    "python",
    "-c",
    "import os,redis\n"
    "r=redis.Redis.from_url(os.environ['FTQ_REDIS_URL'])\n"
    "r.flushall()\n"
    "print('flushed; dbsize', r.dbsize())\n",
]


def coordinator_command(
    p: BenchPoint, run_id: str, meta: dict[str, Any], rate: float | None = None
) -> list[str]:
    """The coordinator's loadgen, then the report dump. The loadgen's exit code is kept:
    1 means "ran, but not exactly-once", and that report is kept too."""
    args = [
        "python", "-m", "bench.loadgen", "--queue", QUEUE,
        "--hosts", "2", "--run-id", run_id, "--expect-workers", str(p.workers),
        "--warmup", str(p.warmup), "--measure", str(p.measure), "--cooldown", str(p.cooldown),
        "--drain-timeout", "600", "--host-timeout", "180",
        *p.args,
        *(["--rate", f"{rate:.0f}"] if rate is not None else []),
        "--meta", json.dumps(meta, separators=(",", ":")), "--out", "/tmp/report.json",
    ]  # fmt: skip
    script = f"{shlex.join(args)}; rc=$?; python -c {shlex.quote(_DUMP)}; exit $rc"
    return ["sh", "-c", script]


def producer_command(run_id: str) -> list[str]:
    """Everything else comes from the coordinator's published spec (ADR-047)."""
    return [
        "python", "-m", "bench.loadgen", "--queue", QUEUE,
        "--producer-only", "--run-id", run_id, "--host-timeout", "300",
    ]  # fmt: skip


def decode_report(lines: Sequence[str]) -> dict[str, Any] | None:
    """The report dumped between REPORT-BEGIN and REPORT-END, or None if it's missing
    or cut short (the task died, or the logs haven't all arrived yet)."""
    chunks: list[str] = []
    inside = ended = False
    for raw in lines:
        line = raw.strip()
        if line == "REPORT-BEGIN":
            inside, chunks = True, []
        elif line == "REPORT-END" and inside:
            ended = True
            break
        elif inside and line.startswith("R:"):
            chunks.append(line[2:])
    if not ended:
        return None
    report: dict[str, Any] = json.loads(gzip.decompress(base64.b64decode("".join(chunks))))
    return report


def scrub(text: str, account_id: str) -> str:
    """Account IDs never go into the repo (CLAUDE.md)."""
    return text.replace(account_id, "<acct>") if account_id else text


# ---------------------------------------------------------------- backends


@dataclass
class PairResult:
    coordinator_exit: int | None
    producer_exit: int | None
    coordinator_log: list[str]
    producer_log: list[str]
    placement: dict[str, str] = field(default_factory=dict)  # role -> host


class Backend(Protocol):
    name: str

    def set_workers(self, n: int) -> None: ...
    def flush(self) -> None: ...
    def snapshot(self) -> dict[str, Any]: ...
    def run_pair(
        self, coordinator: list[str], producer: list[str], within: float
    ) -> PairResult: ...
    def scrub(self, text: str) -> str: ...


Runner = Callable[[list[str]], str]


def _run(cmd: list[str]) -> str:
    return subprocess.run(cmd, check=True, capture_output=True, text=True).stdout


def _wait(what: str, done: Callable[[], bool], within: float, every: float) -> None:
    deadline = time.monotonic() + within
    while not done():
        if time.monotonic() > deadline:
            raise RuntimeError(f"timed out after {within:.0f}s waiting for {what}")
        time.sleep(every)


class AwsBackend:
    """ECS through the AWS CLI. `runner` is injectable so the call sequence can be
    unit-tested with a fake (the real thing runs only in a billable session)."""

    name = "aws"

    def __init__(self, outputs: dict[str, Any], runner: Runner = _run, poll_s: float = 5) -> None:
        self._run = runner
        self._poll = poll_s
        self.cluster: str = outputs["cluster"]
        self.task_def: str = outputs["loadgen_task_definition"]
        self.account = json.loads(self._aws("sts", "get-caller-identity"))["Account"]

    def _aws(self, *args: str) -> str:
        return self._run(["aws", *args, "--output", "json"])

    def _ecs(self, *args: str) -> Any:
        return json.loads(self._aws("ecs", *args, "--cluster", self.cluster))

    def scrub(self, text: str) -> str:
        return scrub(text, self.account)

    def set_workers(self, n: int) -> None:
        self._ecs("update-service", "--service", "worker", "--desired-count", str(n))

        def settled() -> bool:
            svc = self._ecs("describe-services", "--services", "worker")["services"][0]
            return bool(
                svc["runningCount"] == n
                and svc["pendingCount"] == 0
                and len(svc["deployments"]) == 1
            )

        _wait(f"{n} running workers", settled, within=600, every=self._poll)

    def loadgen_instances(self) -> list[str]:
        found = self._ecs("list-container-instances", "--filter", "attribute:ftq.role == loadgen")[
            "containerInstanceArns"
        ]
        arns = sorted(str(a) for a in found)
        if len(arns) < 2:
            raise RuntimeError(f"need 2 loadgen hosts, found {len(arns)}")
        return arns[:2]

    def _start(self, instance: str, command: list[str]) -> str:
        overrides = {"containerOverrides": [{"name": "loadgen", "command": command}]}
        res = self._ecs(
            "start-task", "--task-definition", self.task_def,
            "--container-instances", instance, "--overrides", json.dumps(overrides),
        )  # fmt: skip
        if res.get("failures") or not res.get("tasks"):
            raise RuntimeError(f"start-task failed: {res.get('failures')}")
        return str(res["tasks"][0]["taskArn"])

    def _task(self, arn: str) -> dict[str, Any]:
        found: dict[str, Any] = self._ecs("describe-tasks", "--tasks", arn)["tasks"][0]
        return found

    def _logs(self, arn: str) -> list[str]:
        task_id = arn.rsplit("/", 1)[-1]
        lines: list[str] = []
        token = ""
        while True:
            args = [
                "logs", "get-log-events", "--log-group-name", "/ftq/loadgen",
                "--log-stream-name", f"loadgen/loadgen/{task_id}", "--start-from-head",
            ]  # fmt: skip
            if token:
                args += ["--next-token", token]
            page = json.loads(self._aws(*args))
            lines += [e["message"] for e in page["events"]]
            if not page["events"] or page["nextForwardToken"] == token:
                return lines
            token = page["nextForwardToken"]

    def flush(self) -> None:
        arn = self._start(self.loadgen_instances()[0], FLUSH_COMMAND)
        _wait("the flush task", lambda: self._task(arn)["lastStatus"] == "STOPPED", 300, self._poll)
        code = self._task(arn)["containers"][0].get("exitCode")
        if code != 0:
            raise RuntimeError(f"flush exited {code}: {self._logs(arn)[-5:]}")

    def snapshot(self) -> dict[str, Any]:
        services = self._ecs("describe-services", "--services", "worker", "redis")["services"]
        arns = self._ecs("list-tasks", "--service-name", "worker")["taskArns"]
        tasks = self._ecs("describe-tasks", "--tasks", *arns)["tasks"] if arns else []
        instances: dict[str, Any] = {}
        ci_arns = sorted({t["containerInstanceArn"] for t in tasks})
        if ci_arns:
            for ci in self._ecs("describe-container-instances", "--container-instances",
                                *ci_arns)["containerInstances"]:  # fmt: skip
                attrs = {a["name"]: a.get("value") for a in ci.get("attributes", [])}
                instances[ci["containerInstanceArn"]] = {
                    "ec2_instance_id": ci["ec2InstanceId"],
                    "instance_type": attrs.get("ecs.instance-type"),
                }
        return {
            "taken_at_utc": datetime.now(UTC).isoformat(timespec="seconds"),
            "services": [
                {k: s[k] for k in ("serviceName", "desiredCount", "runningCount", "pendingCount")}
                for s in services
            ],
            "worker_tasks": [
                {
                    "task": t["taskArn"].rsplit("/", 1)[-1],
                    "last_status": t["lastStatus"],
                    **instances.get(t["containerInstanceArn"], {}),
                }
                for t in tasks
            ],
        }

    def run_pair(self, coordinator: list[str], producer: list[str], within: float) -> PairResult:
        a, b = self.loadgen_instances()
        prod = self._start(a, producer)
        coord = self._start(b, coordinator)
        _wait(
            "both loadgen tasks to stop",
            lambda: all(self._task(t)["lastStatus"] == "STOPPED" for t in (prod, coord)),
            within,
            self._poll,
        )
        # awslogs is non-blocking: the last lines can land a few seconds after the stop.
        coord_log: list[str] = []

        def report_arrived() -> bool:
            nonlocal coord_log
            coord_log = self._logs(coord)
            return decode_report(coord_log) is not None or not any(
                line.strip() == "REPORT-BEGIN" for line in coord_log
            )

        _wait("the coordinator's report in CloudWatch", report_arrived, 60, self._poll)
        return PairResult(
            self._task(coord)["containers"][0].get("exitCode"),
            self._task(prod)["containers"][0].get("exitCode"),
            coord_log,
            self._logs(prod),
            {"producer_only": a.rsplit("/", 1)[-1], "coordinator": b.rsplit("/", 1)[-1]},
        )


class LocalBackend:
    """The same steps on local Docker: its own Compose project, Redis on 6392, the
    worker image (make up builds it), and two loadgen containers. Tests the driver,
    the pair, worker-count changes and report saving without AWS."""

    name = "local"
    project = "ftq-awsbench"

    def __init__(self, runner: Runner = _run, concurrency: int = 50) -> None:
        self._run = runner
        self.file = REPO_ROOT / "bench" / "runs" / "awsbench-compose.json"
        self.file.parent.mkdir(parents=True, exist_ok=True)
        env = {"FTQ_REDIS_URL": "redis://redis:6379/0", "FTQ_QUEUE": QUEUE}
        self.file.write_text(json.dumps({
            "name": self.project,
            "services": {
                "redis": {
                    "image": "redis:8.8.3",
                    "command": ["redis-server", "--appendonly", "no", "--save", "",
                                "--maxmemory", "2gb", "--maxmemory-policy", "noeviction"],
                    "ports": ["127.0.0.1:6392:6379"],
                    "healthcheck": {"test": ["CMD", "redis-cli", "ping"], "interval": "1s",
                                    "retries": 30},
                },
                "worker": {
                    "image": "ftq-worker:local",
                    "environment": {**env, "FTQ_CONCURRENCY": str(concurrency),
                                    "FTQ_LOG_LEVEL": "WARNING"},
                    "depends_on": {"redis": {"condition": "service_healthy"}},
                },
                "loadgen": {
                    "image": "ftq-worker:local", "working_dir": "/opt/ftq",
                    "entrypoint": ["timeout", "-k", "30", "1800"],
                    "environment": env, "profiles": ["loadgen"],
                },
            },
        }, indent=1))  # fmt: skip
        self._compose = ["docker", "compose", "-f", str(self.file), "-p", self.project]
        self._run([*self._compose, "up", "-d", "--wait", "redis"])

    def scrub(self, text: str) -> str:
        return text

    def _workers(self) -> list[str]:
        out = self._run([*self._compose, "ps", "-q", "worker"])
        return out.split()

    def set_workers(self, n: int) -> None:
        self._run([*self._compose, "up", "-d", "--no-deps", "--scale", f"worker={n}", "worker"])
        _wait(f"{n} running workers", lambda: len(self._workers()) == n, 120, 1)

    def flush(self) -> None:
        out = self._run([*self._compose, "--profile", "loadgen", "run", "--rm", "-T",
                         "--no-deps", "loadgen", *FLUSH_COMMAND])  # fmt: skip
        if "flushed; dbsize 0" not in out:
            raise RuntimeError(f"flush failed: {out}")

    def snapshot(self) -> dict[str, Any]:
        ids = self._workers()
        return {
            "taken_at_utc": datetime.now(UTC).isoformat(timespec="seconds"),
            "services": [{"serviceName": "worker", "runningCount": len(ids)}],
            "worker_tasks": [{"task": i[:12], "last_status": "RUNNING"} for i in ids],
        }

    def _start(self, name: str, command: list[str]) -> None:
        self._run([*self._compose, "--profile", "loadgen", "run", "-d", "--no-deps",
                   "--name", name, "loadgen", *command])  # fmt: skip

    def run_pair(self, coordinator: list[str], producer: list[str], within: float) -> PairResult:
        suffix = uuid.uuid4().hex[:6]
        names = {
            "producer_only": f"awsbench-prod-{suffix}",
            "coordinator": f"awsbench-coord-{suffix}",
        }
        self._start(names["producer_only"], producer)
        self._start(names["coordinator"], coordinator)

        def exited(name: str) -> bool:
            state = self._run(["docker", "inspect", "-f", "{{.State.Status}}", name]).strip()
            return state == "exited"

        _wait("both loadgen containers", lambda: all(exited(n) for n in names.values()), within, 1)
        result = []
        for n in (names["coordinator"], names["producer_only"]):
            code = int(self._run(["docker", "inspect", "-f", "{{.State.ExitCode}}", n]))
            logs = subprocess.run(["docker", "logs", n], capture_output=True, text=True)
            result.append((code, (logs.stdout + logs.stderr).splitlines()))
            self._run(["docker", "rm", n])
        (c_code, c_log), (p_code, p_log) = result
        return PairResult(c_code, p_code, c_log, p_log, names)

    def down(self) -> None:
        self._run([*self._compose, "--profile", "loadgen", "down", "-v", "--remove-orphans"])


# ---------------------------------------------------------------- the driver


def _git_rev() -> str:
    try:
        return _run(["git", "rev-parse", "--short", "HEAD"]).strip()
    except (OSError, subprocess.CalledProcessError):
        return "unknown"


def _headline_rates(out_dir: Path) -> list[float]:
    return [
        float(json.loads(f.read_text())["throughput"]["completed_per_s"])
        for f in sorted((out_dir / "headline").glob("*.json"))
        if not f.name.endswith(".services.json")
    ]


def run_session(
    backend: Backend,
    points: Sequence[BenchPoint],
    out_dir: Path,
    deadline_s: float,
    redo: bool = False,
    clock: Callable[[], float] = time.monotonic,
) -> dict[str, str]:
    """Run each point (see the module docstring); returns label -> outcome. Stops
    starting points once the next one wouldn't finish before `deadline_s`."""
    started = clock()
    outcomes: dict[str, str] = {}
    for p in points:
        key = f"{p.suite}/{p.label}"
        out = out_dir / p.suite / f"{p.label}.json"
        if out.exists() and not redo:
            outcomes[key] = "skipped (saved)"
            continue
        if clock() - started + p.seconds > deadline_s:
            outcomes[key] = "not run (deadline)"
            log.warning("%s: not started, the session deadline is near", key)
            continue
        rate = backpressure_rate(_headline_rates(out_dir), p.overload) if p.overload else None
        run_id = f"{p.suite}-{p.label}-{uuid.uuid4().hex[:8]}"
        log.info("%s: %d workers%s", key, p.workers, f", {rate:.0f}/s offered" if rate else "")
        backend.set_workers(0)
        backend.flush()
        backend.set_workers(p.workers)
        snapshot = backend.snapshot()
        env = ENVIRONMENT if backend.name == "aws" else "local Docker (driver test)"
        meta = {
            "suite": p.suite, "label": p.label, "workers": p.workers,
            "backend": backend.name, "environment": env,
            "date_utc": datetime.now(UTC).isoformat(timespec="seconds"), "git": _git_rev(),
            "run_id": run_id,
        }  # fmt: skip
        pair = backend.run_pair(
            coordinator_command(p, run_id, meta, rate),
            producer_command(run_id),
            within=p.warmup + p.measure + p.cooldown + 1200,
        )
        out.parent.mkdir(parents=True, exist_ok=True)
        base = out.with_suffix("")
        base.with_name(f"{p.label}.services.json").write_text(
            backend.scrub(json.dumps(snapshot, indent=1)) + "\n"
        )
        base.with_name(f"{p.label}.coordinator.txt").write_text(
            backend.scrub("\n".join(x for x in pair.coordinator_log if not x.startswith("R:")))
        )
        base.with_name(f"{p.label}.producer.txt").write_text(
            backend.scrub("\n".join(pair.producer_log))
        )
        report = decode_report(pair.coordinator_log)
        if report is None:
            outcomes[key] = f"FAILED: no report (coordinator exit {pair.coordinator_exit})"
            log.error("%s: %s", key, outcomes[key])
            continue
        report["meta"]["placement"] = pair.placement
        report["meta"]["producer_exit"] = pair.producer_exit
        out.write_text(backend.scrub(json.dumps(report, indent=1)) + "\n")
        once = report["exactly_once"]
        outcomes[key] = (
            f"{report['throughput']['completed_per_s']:.0f}/s, exactly-once {once['ok']}, "
            f"hosts {report['producers']['hosts']['reported']}/2, depth min "
            f"{report['depth_in_window']['min']}"
        )
        log.info("%s: %s", key, outcomes[key])
    return outcomes


# ---------------------------------------------------------------- CLI


def _spec(a: argparse.Namespace) -> SessionSpec:
    return SessionSpec(
        worker_counts=tuple(a.worker_counts), scaling_measure=a.scaling_measure,
        headline_workers=a.headline_workers, headline_measure=a.headline_measure,
        headline_repeats=a.headline_repeats, backpressure_measure=a.backpressure_measure,
        high_watermark=a.high_watermark, low_watermark=a.low_watermark,
        warmup=a.warmup, cooldown=a.cooldown, processes=a.processes, suites=tuple(a.suites),
    )  # fmt: skip


def cmd_plan(points: Sequence[BenchPoint], per_hour: float) -> None:
    total = session_seconds(points)
    for p in points:
        extra = f" (offer {p.overload:g} x headline)" if p.overload else ""
        print(f"  {p.suite:<13}{p.label:<12}{p.workers:>3} workers  measure {p.measure:>4} s  "
              f"~{p.seconds / 60:4.1f} min{extra}")  # fmt: skip
    print(
        f"  points: {len(points)}, {sum(p.seconds for p in points) / 60:.0f} min; "
        f"+ session overhead {SESSION_OVERHEAD_S / 60:.0f} min = {total / 60:.0f} min"
    )
    if per_hour:
        print(f"  at ${per_hour:.4f}/h: ${per_hour * total / 3600:.2f}")


def main(argv: list[str] | None = None) -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
    d = SessionSpec()
    p = argparse.ArgumentParser(prog="python -m deploy.bench", description=__doc__.split("\n")[0])
    p.add_argument("cmd", choices=["plan", "run"])
    p.add_argument("--backend", choices=["aws", "local"], default="aws")
    p.add_argument("--out", type=Path, default=RESULTS)
    p.add_argument("--deadline-min", type=float, default=120)
    p.add_argument("--redo", action="store_true", help="rerun points that have results")
    p.add_argument("--per-hour", type=float, default=0.0, help="$/h, for the plan's cost line")
    p.add_argument("--suites", nargs="+", default=list(d.suites))
    p.add_argument("--worker-counts", type=int, nargs="+", default=list(d.worker_counts))
    p.add_argument("--scaling-measure", type=int, default=d.scaling_measure)
    p.add_argument("--headline-workers", type=int, default=d.headline_workers)
    p.add_argument("--headline-measure", type=int, default=d.headline_measure)
    p.add_argument("--headline-repeats", type=int, default=d.headline_repeats)
    p.add_argument("--backpressure-measure", type=int, default=d.backpressure_measure)
    p.add_argument("--high-watermark", type=int, default=d.high_watermark)
    p.add_argument("--low-watermark", type=int, default=d.low_watermark)
    p.add_argument("--warmup", type=int, default=d.warmup)
    p.add_argument("--cooldown", type=int, default=d.cooldown)
    p.add_argument("--processes", type=int, default=d.processes)
    a = p.parse_args(argv)
    points = session_points(_spec(a))
    if a.cmd == "plan":
        cmd_plan(points, a.per_hour)
        return
    backend: Backend
    if a.backend == "aws":
        from deploy.aws import _stack_outputs

        backend = AwsBackend(_stack_outputs())
    else:
        backend = LocalBackend()
    try:
        outcomes = run_session(backend, points, a.out, a.deadline_min * 60, a.redo)
    finally:
        if isinstance(backend, LocalBackend):
            backend.down()
    for key, outcome in outcomes.items():
        print(f"  {key:<28} {outcome}")
    if any(o.startswith("FAILED") for o in outcomes.values()):
        raise SystemExit(1)


if __name__ == "__main__":
    main()
