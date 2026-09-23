"""The Phase 8 driver (deploy/bench.py), without AWS: the session plan, the commands it
sends, the report's trip through the task log, the order of steps per point, and the
ECS calls against a fake `aws` (pure logic: the real ECS run is the billable session)."""

import contextlib
import io
import json
import shlex
import subprocess
from pathlib import Path
from typing import Any

import pytest

from deploy import bench
from deploy.bench import (
    AwsBackend,
    BenchPoint,
    PairResult,
    SessionSpec,
    backpressure_rate,
    coordinator_command,
    decode_report,
    producer_command,
    run_session,
    session_points,
    session_seconds,
)

# ---------------------------------------------------------------- plan


def test_the_default_session_is_the_spec_phase_8_plan() -> None:
    points = session_points(SessionSpec())
    labels = [f"{p.suite}/{p.label}" for p in points]
    assert labels == [
        "scaling/w01", "scaling/w02", "scaling/w04", "scaling/w08", "scaling/w12",
        "headline/w12_r1", "headline/w12_r2", "headline/w12_r3",
        "backpressure/w12_reject", "backpressure/w12_block",
    ]  # fmt: skip
    assert all(p.measure >= 180 for p in points if p.suite == "scaling")  # ~3 min each
    assert all(p.measure >= 300 for p in points if p.suite == "headline")  # >= 5 min
    assert all(p.overload == 1.5 for p in points if p.suite == "backpressure")
    # 10 points of warmup + measure + cooldown + overhead, plus the session overhead.
    assert session_seconds(points) == sum(p.seconds for p in points) + bench.SESSION_OVERHEAD_S


def test_backpressure_offers_a_multiple_of_the_median_headline() -> None:
    assert backpressure_rate([14_000, 15_000, 20_000], 1.5) == 22_500
    with pytest.raises(ValueError, match="headline"):
        backpressure_rate([], 1.5)


# ---------------------------------------------------------------- commands


def _point(**kw: Any) -> BenchPoint:
    base: dict[str, Any] = {
        "suite": "scaling", "label": "w04", "workers": 4, "warmup": 20, "measure": 180,
        "cooldown": 5, "args": ("--processes", "4", "--rate", "0"),
    }  # fmt: skip
    return BenchPoint(**{**base, **kw})


def test_the_coordinator_command_survives_the_shell() -> None:
    meta = {"label": "w12_reject", "tricky": 'it\'s "quoted" $HOME; rm -rf /'}
    point = _point(args=("--processes", "4", "--backpressure", "reject"))
    sh, flag, script = coordinator_command(point, "run-1", meta, rate=22500.4)
    assert (sh, flag) == ("sh", "-c")
    loadgen, rest = script.split("; rc=$?; ", 1)
    args = shlex.split(loadgen)
    assert args[:3] == ["python", "-m", "bench.loadgen"]
    assert args[args.index("--hosts") + 1] == "2"
    assert args[args.index("--run-id") + 1] == "run-1"
    assert args[args.index("--expect-workers") + 1] == "4"
    assert args[args.index("--rate") + 1] == "22500"
    assert args[-4:] == ["--meta", json.dumps(meta, separators=(",", ":")), "--out",
                         "/tmp/report.json"]  # fmt: skip
    assert rest.endswith("exit $rc")  # the loadgen's exit code is what the task returns
    # A saturation point sends only its own --rate 0.
    _, _, sat = coordinator_command(_point(), "run-2", {})
    sat_args = shlex.split(sat.split("; rc=$?")[0])
    assert sat_args.count("--rate") == 1 and sat_args[sat_args.index("--rate") + 1] == "0"


def test_the_producer_takes_everything_but_the_run_id_from_the_coordinator() -> None:
    cmd = producer_command("run-1")
    assert cmd[cmd.index("--run-id") + 1] == "run-1"
    assert "--producer-only" in cmd
    for flag in ("--rate", "--processes", "--measure", "--hosts"):
        assert flag not in cmd


# ---------------------------------------------------------------- the report's trip


def _dumped(report: dict[str, Any], tmp_path: Path) -> list[str]:
    """Run the real dump code on a saved report and return what it prints."""
    path = tmp_path / "report.json"
    path.write_text(json.dumps(report))
    out = io.StringIO()
    with contextlib.redirect_stdout(out):
        exec(bench._DUMP.replace("/tmp/report.json", str(path)), {})
    return out.getvalue().splitlines()


def test_the_report_survives_the_dump_and_the_log(tmp_path: Path) -> None:
    report = {"throughput": {"completed_per_s": 12345.6}, "big": "x" * 300_000}
    lines = _dumped(report, tmp_path)
    assert all(len(line) < 256 * 1024 for line in lines)  # CloudWatch's event limit
    # Log lines interleaved with the loadgen's own output, as a task log has them.
    assert decode_report(["loadgen producing", *lines, "trailing"]) == report


def test_a_cut_off_report_is_no_report(tmp_path: Path) -> None:
    lines = _dumped({"a": 1}, tmp_path)
    assert decode_report(lines[:-1]) is None  # no REPORT-END: the log isn't complete
    assert decode_report([]) is None


# ---------------------------------------------------------------- the session loop


class FakeBackend:
    name = "fake"

    def __init__(self, report: dict[str, Any] | None) -> None:
        self.calls: list[tuple[Any, ...]] = []
        self.report = report
        self.workers = 0

    def wait_for_redis(self) -> None:
        self.calls.append(("wait_for_redis",))

    def set_workers(self, n: int) -> None:
        self.calls.append(("set_workers", n))
        self.workers = n

    def flush(self) -> None:
        assert self.workers == 0, "flushed while workers were running"
        self.calls.append(("flush",))

    def snapshot(self) -> dict[str, Any]:
        self.calls.append(("snapshot", self.workers))
        return {"worker_tasks": [{"task": f"t{i}"} for i in range(self.workers)],
                "account": "123456789012"}  # fmt: skip

    def run_pair(self, coordinator: list[str], producer: list[str], within: float) -> PairResult:
        self.calls.append(("run_pair", coordinator[2].split("--run-id ")[1].split()[0]))
        dump: list[str] = []
        if self.report is not None:
            import base64
            import gzip

            data = base64.b64encode(gzip.compress(json.dumps(self.report).encode())).decode()
            dump = ["REPORT-BEGIN", "R:" + data, "REPORT-END"]
        return PairResult(0, 0, ["coordinator line", *dump], ["producer line 123456789012"],
                          {"producer_only": "i-a", "coordinator": "i-b"})  # fmt: skip

    def scrub(self, text: str) -> str:
        return text.replace("123456789012", "<acct>")


def _report() -> dict[str, Any]:
    return {
        "meta": {}, "throughput": {"completed_per_s": 15000.0},
        "exactly_once": {"ok": True}, "producers": {"hosts": {"reported": 2}},
        "depth_in_window": {"min": 1234},
    }  # fmt: skip


def test_each_point_resets_redis_with_no_workers_then_scales_and_saves(tmp_path: Path) -> None:
    fake = FakeBackend(_report())
    points = session_points(SessionSpec(worker_counts=(1, 4), suites=("scaling",)))
    outcomes = run_session(fake, points, tmp_path, deadline_s=1e9)
    steps = [c[0] if c[0] != "set_workers" else f"set_workers({c[1]})" for c in fake.calls]
    assert steps == [
        "wait_for_redis",
        "set_workers(0)", "flush", "set_workers(1)", "snapshot", "run_pair",
        "set_workers(0)", "flush", "set_workers(4)", "snapshot", "run_pair",
    ]  # fmt: skip
    assert ("snapshot", 4) in fake.calls  # the evidence is taken with the fleet running
    run_ids = [c[1] for c in fake.calls if c[0] == "run_pair"]
    assert len(set(run_ids)) == 2  # a fresh run id every point (SET NX would refuse reuse)
    saved = json.loads((tmp_path / "scaling" / "w04.json").read_text())
    assert saved["meta"]["placement"] == {"producer_only": "i-a", "coordinator": "i-b"}
    services = (tmp_path / "scaling" / "w04.services.json").read_text()
    assert "123456789012" not in services and "<acct>" in services
    assert "<acct>" in (tmp_path / "scaling" / "w04.producer.txt").read_text()
    assert "R:" not in (tmp_path / "scaling" / "w04.coordinator.txt").read_text()
    assert outcomes["scaling/w04"].startswith("15000/s, exactly-once True, hosts 2/2")


def test_saved_points_are_skipped_and_the_deadline_stops_new_ones(tmp_path: Path) -> None:
    (tmp_path / "scaling").mkdir()
    (tmp_path / "scaling" / "w01.json").write_text("{}")
    fake = FakeBackend(_report())
    points = session_points(SessionSpec(worker_counts=(1, 2, 4), suites=("scaling",)))
    ticks = iter([0.0, 0.0, 10_000.0])  # start, w02 fits, then w04 is past the deadline
    outcomes = run_session(fake, points, tmp_path, deadline_s=1_000, clock=lambda: next(ticks))
    assert outcomes == {
        "scaling/w01": "skipped (saved)",
        "scaling/w02": outcomes["scaling/w02"],
        "scaling/w04": "not run (deadline)",
    }
    assert [c for c in fake.calls if c[0] == "set_workers"] == [("set_workers", 0),
                                                                 ("set_workers", 2)]  # fmt: skip


def test_a_missing_report_is_a_failure_and_saves_no_result(tmp_path: Path) -> None:
    fake = FakeBackend(report=None)
    points = session_points(SessionSpec(worker_counts=(2,), suites=("scaling",)))
    outcomes = run_session(fake, points, tmp_path, deadline_s=1e9)
    assert outcomes["scaling/w02"].startswith("FAILED: no report")
    assert not (tmp_path / "scaling" / "w02.json").exists()
    assert (tmp_path / "scaling" / "w02.coordinator.txt").exists()  # the evidence stays


def test_backpressure_points_offer_1_5x_the_saved_headline_median(tmp_path: Path) -> None:
    (tmp_path / "headline").mkdir()
    for i, rate in enumerate([14_000, 16_000, 15_000]):
        (tmp_path / "headline" / f"w12_r{i + 1}.json").write_text(
            json.dumps({"throughput": {"completed_per_s": rate}})
        )
    (tmp_path / "headline" / "w12_r1.services.json").write_text("{}")  # not a result
    fake = FakeBackend(_report())
    sent: list[list[str]] = []
    original = fake.run_pair

    def capture(c: list[str], p: list[str], within: float) -> PairResult:
        sent.append(c)
        return original(c, p, within)

    fake.run_pair = capture  # type: ignore[method-assign,assignment]
    points = session_points(SessionSpec(suites=("backpressure",)))
    run_session(fake, points, tmp_path, deadline_s=1e9)
    for c in sent:
        args = shlex.split(c[2].split("; rc=$?")[0])
        assert args[args.index("--rate") + 1] == "22500"  # 1.5 x 15,000


# ---------------------------------------------------------------- ECS calls (fake aws)


ACCT = "123456789012"


class FakeAws:
    """Answers the AWS CLI calls AwsBackend makes; records them."""

    def __init__(self) -> None:
        self.calls: list[list[str]] = []
        self.desired = 0
        self.started: list[tuple[str, list[str]]] = []
        # The Redis task, one state per poll (the last one repeats): (running count,
        # task lastStatus, healthStatus). Default: up and healthy from the start.
        self.redis_states: list[tuple[int, str, str]] = [(1, "RUNNING", "HEALTHY")]
        self.redis_polls = 0

    def _redis(self) -> tuple[int, str, str]:
        return self.redis_states[min(self.redis_polls, len(self.redis_states)) - 1]

    def __call__(self, cmd: list[str]) -> str:
        self.calls.append(cmd)
        args = cmd[1:]
        op = args[1] if args[0] == "ecs" else args[0]

        def arg(flag: str) -> str:
            return args[args.index(flag) + 1]

        if args[:2] == ["sts", "get-caller-identity"]:
            return json.dumps({"Account": "123456789012"})
        if op == "update-service":
            self.desired = int(arg("--desired-count"))
            return "{}"
        if op == "describe-services" and arg("--services") == "redis":
            self.redis_polls += 1  # each describe-services poll advances the state
            n = self._redis()[0]
            return json.dumps(
                {
                    "services": [
                        {
                            "serviceName": "redis",
                            "desiredCount": 1,
                            "runningCount": n,
                            "pendingCount": 1 - n,
                        }
                    ]
                }
            )
        if op == "list-tasks" and arg("--service-name") == "redis":
            return json.dumps({"taskArns": ["arn:task/redis"] if self._redis()[0] else []})
        if op == "describe-tasks" and arg("--tasks") == "arn:task/redis":
            _, status, health = self._redis()
            return json.dumps({"tasks": [{"lastStatus": status, "healthStatus": health,
                                          "containers": [{}]}]})  # fmt: skip
        if op == "describe-services":
            n = self.desired
            td = f"arn:aws:ecs:r:{ACCT}:task-definition/ftq-worker:7"
            svc = {"serviceName": "worker", "desiredCount": n, "runningCount": n,
                   "pendingCount": 0, "deployments": [{}], "taskDefinition": td}  # fmt: skip
            return json.dumps({"services": [svc]})
        if op == "list-tasks":
            return json.dumps({"taskArns": []})
        if op == "describe-task-definition":
            assert "--cluster" not in args  # the call takes no cluster
            return json.dumps({"taskDefinition": {
                "taskDefinitionArn": arg("--task-definition"),
                "containerDefinitions": [{"image": "123456789012.dkr.ecr.r/ftq:3d43177",
                    "environment": [{"name": "FTQ_CONCURRENCY", "value": "50"},
                                    {"name": "FTQ_REDIS_URL", "value": "redis://10.0.0.1:6379/0"},
                                    {"name": "FTQ_LOG_LEVEL", "value": "WARNING"}]}],
            }})  # fmt: skip
        if op == "list-container-instances":
            return json.dumps({"containerInstanceArns": ["arn:ci/B", "arn:ci/A", "arn:ci/C"]})
        if op == "start-task":
            command = json.loads(arg("--overrides"))["containerOverrides"][0]["command"]
            self.started.append((arg("--container-instances"), command))
            return json.dumps({"tasks": [{"taskArn": f"arn:task/{len(self.started)}"}],
                               "failures": []})  # fmt: skip
        if op == "describe-tasks":
            return json.dumps(
                {"tasks": [{"lastStatus": "STOPPED", "containers": [{"exitCode": 0}]}]}
            )
        if args[:2] == ["logs", "get-log-events"]:
            # Every task exits 0, so like a real run each stream holds a dumped report
            # (the driver waits for one after exit 0). First page only; then the end.
            lines = [] if "--next-token" in args else _dump_lines(_report())
            return json.dumps({"events": [{"message": m} for m in lines],
                               "nextForwardToken": "t"})  # fmt: skip
        raise AssertionError(f"unexpected call {cmd}")


def test_the_pair_runs_on_two_different_loadgen_hosts() -> None:
    fake = FakeAws()
    backend = AwsBackend({"cluster": "ftq", "loadgen_task_definition": "td"}, fake, poll_s=0)
    pair = backend.run_pair(["coord"], ["prod"], within=5)
    assert decode_report(pair.coordinator_log) == _report()
    (host_a, cmd_a), (host_b, cmd_b) = fake.started
    assert host_a != host_b
    assert {host_a, host_b} <= {"arn:ci/A", "arn:ci/B", "arn:ci/C"}
    assert (cmd_a, cmd_b) == (["prod"], ["coord"])  # the producer first: it waits for the spec
    # start-task on a named instance, never run-task (which can't guarantee distinct hosts).
    assert not any("run-task" in c for c in fake.calls)


def test_setting_workers_waits_for_the_service_to_settle() -> None:
    fake = FakeAws()
    backend = AwsBackend({"cluster": "ftq", "loadgen_task_definition": "td"}, fake, poll_s=0)
    backend.set_workers(12)
    update = next(c for c in fake.calls if "update-service" in c)
    assert update[update.index("--desired-count") + 1] == "12"
    assert "describe-services" in fake.calls[-1]


def test_the_driver_waits_for_a_running_healthy_redis_task() -> None:
    """Phase 8 part 3: the driver ran 13 s after apply and FLUSHALL was refused, because
    the Redis task wasn't running yet. It must wait through every earlier state."""
    fake = FakeAws()
    fake.redis_states = [
        (0, "", ""),                       # service created, no task yet
        (0, "", ""),
        (1, "PENDING", "UNKNOWN"),         # counted, but the container is still starting
        (1, "RUNNING", "UNKNOWN"),         # running; the health check hasn't passed yet
        (1, "RUNNING", "HEALTHY"),
    ]  # fmt: skip
    backend = AwsBackend({"cluster": "ftq", "loadgen_task_definition": "td"}, fake, poll_s=0)
    backend.wait_for_redis(within=5)
    assert fake.redis_polls == 5  # it returned only on the last state, never before


def test_a_redis_task_that_never_gets_healthy_stops_the_session() -> None:
    fake = FakeAws()
    fake.redis_states = [(1, "RUNNING", "UNHEALTHY")]
    backend = AwsBackend({"cluster": "ftq", "loadgen_task_definition": "td"}, fake, poll_s=0)
    with pytest.raises(RuntimeError, match="Redis task"):
        backend.wait_for_redis(within=0.05)
    assert not any("start-task" in c for c in fake.calls)  # no flush was attempted


def test_redis_is_waited_for_once_and_only_when_a_point_runs(tmp_path: Path) -> None:
    (tmp_path / "scaling").mkdir()
    (tmp_path / "scaling" / "w01.json").write_text("{}")
    fake = FakeBackend(_report())
    points = session_points(SessionSpec(worker_counts=(1,), suites=("scaling",)))
    run_session(fake, points, tmp_path, deadline_s=1e9)
    assert fake.calls == []  # every point saved: nothing to wait for
    points = session_points(SessionSpec(worker_counts=(1, 2, 4), suites=("scaling",)))
    run_session(fake, points, tmp_path, deadline_s=1e9)
    assert [c[0] for c in fake.calls].count("wait_for_redis") == 1
    assert fake.calls[0] == ("wait_for_redis",)  # before the first set_workers or flush


def test_the_snapshot_records_the_workers_in_flight_cap_and_image() -> None:
    """SPEC §9: the throughput claim states the in-flight cap. Phase 8's reports didn't
    carry it; the snapshot now does, read from the running task definition."""
    fake = FakeAws()
    backend = AwsBackend({"cluster": "ftq", "loadgen_task_definition": "td"}, fake, poll_s=0)
    fake.desired = 12
    snap = backend.snapshot()
    assert snap["worker_container"] == {
        "task_definition": "ftq-worker:7",
        "image_tag": "3d43177",
        "env": {"FTQ_CONCURRENCY": "50", "FTQ_LOG_LEVEL": "WARNING"},  # no Redis address
    }


def test_a_failed_flush_stops_the_session() -> None:
    fake = FakeAws()
    backend = AwsBackend({"cluster": "ftq", "loadgen_task_definition": "td"}, fake, poll_s=0)
    original = fake.__call__

    def failing(cmd: list[str]) -> str:
        if "describe-tasks" in cmd:
            return json.dumps(
                {"tasks": [{"lastStatus": "STOPPED", "containers": [{"exitCode": 1}]}]}
            )
        return original(cmd)

    backend._run = failing
    with pytest.raises(RuntimeError, match="flush exited 1"):
        backend.flush()


def _dump_lines(report: dict[str, Any]) -> list[str]:
    import base64
    import gzip

    data = base64.b64encode(gzip.compress(json.dumps(report).encode())).decode()
    return ["loadgen completed", "REPORT-BEGIN", "R:" + data, "REPORT-END"]


def test_a_log_that_has_not_arrived_yet_is_waited_for() -> None:
    """Phase 8 scaling/w02: CloudWatch had delivered 0 lines when the driver first read
    the coordinator's log. Exit 0 means a report is coming: wait for it."""
    fake = FakeAws()
    reads = iter([[], [], _dump_lines({"ok": 1})])  # empty twice, then the whole log
    original = fake.__call__

    def slow_logs(cmd: list[str]) -> str:
        if cmd[1:3] == ["logs", "get-log-events"]:
            if "--next-token" in cmd:
                return json.dumps({"events": [], "nextForwardToken": "t"})
            lines = next(reads, [])
            return json.dumps({"events": [{"message": m} for m in lines],
                               "nextForwardToken": "t"})  # fmt: skip
        return original(cmd)

    backend = AwsBackend({"cluster": "ftq", "loadgen_task_definition": "td"}, slow_logs, 0)
    pair = backend.run_pair(["coord"], ["prod"], within=5)
    assert decode_report(pair.coordinator_log) == {"ok": 1}


def test_recover_reads_back_the_same_runs_report_and_marks_it(tmp_path: Path) -> None:
    (tmp_path / "scaling").mkdir()
    (tmp_path / "scaling" / "w02.services.json").write_text("{}")  # ran, but no report
    mine = {**_report(), "meta": {"suite": "scaling", "label": "w02", "run_id": "rid"}}
    other = {**_report(), "meta": {"suite": "headline", "label": "w02", "run_id": "x"}}
    streams = {"loadgen/loadgen/aaa": _dump_lines(other), "loadgen/loadgen/bbb": _dump_lines(mine),
               "loadgen/loadgen/ccc": ["producing (run rid)"]}  # fmt: skip
    fake = FakeAws()
    original = fake.__call__

    def logs(cmd: list[str]) -> str:
        if cmd[1:3] == ["logs", "filter-log-events"]:
            pattern = cmd[cmd.index("--filter-pattern") + 1]
            coordinators = ["loadgen/loadgen/aaa", "loadgen/loadgen/bbb"]
            names = ["loadgen/loadgen/ccc"] if "rid" in pattern else coordinators
            return json.dumps({"events": [{"logStreamName": n} for n in names]})
        if cmd[1:3] == ["logs", "get-log-events"]:
            name = cmd[cmd.index("--log-stream-name") + 1]
            if "--next-token" in cmd:
                return json.dumps({"events": [], "nextForwardToken": "t"})
            return json.dumps({"events": [{"message": m} for m in streams[name]],
                               "nextForwardToken": "t"})  # fmt: skip
        return original(cmd)

    backend = AwsBackend({"cluster": "ftq", "loadgen_task_definition": "td"}, logs, 0)
    outcomes = bench.recover(backend, tmp_path, since_ms=0)
    assert outcomes["scaling/w02"].startswith("recovered: 15000/s")
    saved = json.loads((tmp_path / "scaling" / "w02.json").read_text())
    assert saved["meta"]["run_id"] == "rid"  # the scaling run's, not headline's w02
    assert "loadgen/loadgen/bbb" in saved["meta"]["recovered"]
    assert "run rid" in (tmp_path / "scaling" / "w02.producer.txt").read_text()


# ---------------------------------------------------------------- transient CLI errors


def _failing(fake: FakeAws, op: str, times: int, stderr: str = "") -> Any:
    """`fake`, except the first `times` calls of `op` exit 255 with `stderr`."""
    left = {"n": times}

    def run(cmd: list[str]) -> str:
        if op in cmd and left["n"] > 0:
            left["n"] -= 1
            fake.calls.append(cmd)
            raise bench.CommandError(255, cmd, "", stderr)
        return fake(cmd)

    return run


def _backend(runner: Any) -> AwsBackend:
    return AwsBackend({"cluster": "ftq", "loadgen_task_definition": "td"}, runner, 0, retry_s=0)


def test_a_read_only_poll_survives_transient_cli_errors() -> None:
    """Phase 8 part 2: one describe-tasks exit 255 ended the session, twice."""
    fake = FakeAws()
    backend = _backend(_failing(fake, "describe-tasks", times=2))
    pair = backend.run_pair(["coord"], ["prod"], within=5)
    assert pair.coordinator_exit == 0
    assert sum("describe-tasks" in c for c in fake.calls) > 2  # the 2 failures were retried


def test_a_read_only_poll_gives_up_after_three_attempts_with_stderr() -> None:
    fake = FakeAws()
    backend = _backend(_failing(fake, "describe-services", times=99, stderr="Throttling"))
    with pytest.raises(bench.CommandError, match="Throttling"):
        backend.set_workers(3)
    assert sum("describe-services" in c for c in fake.calls) == bench.READ_ATTEMPTS == 3


@pytest.mark.parametrize("op", ["start-task", "update-service"])
def test_a_call_that_changes_state_is_never_retried(op: str) -> None:
    """A retried start-task could start a second loadgen; a retried update-service is
    only harmless by luck. Neither is on the read-only allowlist."""
    fake = FakeAws()
    backend = _backend(_failing(fake, op, times=1))
    with pytest.raises(bench.CommandError):
        if op == "start-task":
            backend.run_pair(["coord"], ["prod"], within=5)
        else:
            backend.set_workers(3)
    assert sum(op in c for c in fake.calls) == 1


def test_a_failed_command_carries_its_stderr() -> None:
    with pytest.raises(bench.CommandError, match=r"exit status 255.*stderr: boom") as e:
        bench._run(["sh", "-c", "echo boom >&2; exit 255"])
    assert isinstance(e.value, subprocess.CalledProcessError)  # old handlers still work


def test_the_snapshot_is_saved_before_the_pair_runs(tmp_path: Path) -> None:
    """Phase 8 part 2: a crash during w12_reject's pair lost its snapshot."""

    class Crashing(FakeBackend):
        def run_pair(self, coordinator: list[str], producer: list[str], within: float) -> Any:
            raise RuntimeError("driver crashed mid-point")

    points = session_points(SessionSpec(worker_counts=(4,), suites=("scaling",)))
    with pytest.raises(RuntimeError, match="mid-point"):
        run_session(Crashing(_report()), points, tmp_path, deadline_s=1e9)
    snap = json.loads((tmp_path / "scaling" / "w04.services.json").read_text())
    assert len(snap["worker_tasks"]) == 4
    assert snap["account"] == "<acct>"  # scrubbed like every other saved file
    assert not (tmp_path / "scaling" / "w04.json").exists()  # still resumable
