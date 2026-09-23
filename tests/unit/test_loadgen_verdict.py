"""The benchmark's exactly-once verdict (bench/loadgen.py `_report`). Every Phase 8 point
reports "exactly-once True"; these pin each way the verdict must turn False. (The
integration tests cover "not drained" and "missing" end to end; nothing covered a
duplicate result or a DLQ entry until the Phase 6-8 review.)

The check compares counts: accepted jobs vs distinct job_ids in the results log, no
job_id twice, an empty DLQ, a drained queue, every host reported. It doesn't compare
job_id sets (the chaos verifier does); on a FLUSHALLed Redis with one producer program,
a result can only belong to a job this run enqueued.
"""

from collections import Counter
from typing import Any

from bench.analysis import Window
from bench.loadgen import LoadSpec, _report

# Every Phase 8 point reports "exactly-once True". These pin each way the verdict must
# turn False (the integration tests cover "not drained" and "missing" end to end).


def _verdict(per_job: dict[str, int], dlq: int = 0, drained: bool = True) -> dict[str, Any]:
    spec = LoadSpec(redis_url="", warmup=0, measure=1, cooldown=0)
    produced = [{
        "per_second": {0: [3, 3, 0]}, "enqueue_call_hist_ms": {}, "batch_sizes": {},
        "offered": 3, "accepted": 3, "rejected": 0, "blocked": 0, "blocked_seconds": 0.0,
        "cpu_busy": 0.1, "max_lag_s": 0.0, "start_late_s": 0.0, "depth_waits": 0,
    }]  # fmt: skip
    samples = [{"t_ms": t, "depth": 0, "redis_cpu_main_s": 0.0, "redis_cpu_s": 0.0,
                "used_memory": 0} for t in (0, 2000)]  # fmt: skip
    report = _report(
        spec, {}, {}, 1, Window.of_run(0, 0, 1), 0, samples, produced,
        [(0, 5)] * sum(per_job.values()), Counter(per_job), Counter(), {}, dlq, drained, 1.0, {},
    )  # fmt: skip
    once: dict[str, Any] = report["exactly_once"]
    return once


def test_three_accepted_three_distinct_results_is_exactly_once() -> None:
    assert _verdict({"a": 1, "b": 1, "c": 1})["ok"]


def test_a_duplicate_result_is_not_exactly_once() -> None:
    once = _verdict({"a": 2, "b": 1, "c": 1})
    assert once["duplicate_results"] == 1 and once["missing"] == 0
    assert not once["ok"]


def test_a_dead_lettered_job_is_not_exactly_once() -> None:
    assert not _verdict({"a": 1, "b": 1, "c": 1}, dlq=1)["ok"]


def test_a_missing_or_undrained_job_is_not_exactly_once() -> None:
    assert _verdict({"a": 1, "b": 1})["missing"] == 1
    assert not _verdict({"a": 1, "b": 1})["ok"]
    assert not _verdict({"a": 1, "b": 1, "c": 1}, drained=False)["ok"]
