"""The benchmark's arithmetic (bench/analysis.py). Every reported percentile, rate, and
CPU fraction goes through these functions, so their edge cases are pinned here. Plus
the load generator's payload sizing, the other pure piece of bench/."""

import json

import pytest

from bench import analysis
from bench.analysis import Window
from bench.loadgen import LoadSpec, command_costs, make_payload


def test_percentile_is_nearest_rank_and_never_interpolates() -> None:
    hist = {1: 50, 2: 45, 10: 4, 100: 1}  # 100 samples
    assert analysis.percentile(hist, 50) == 1  # the 50th value
    assert analysis.percentile(hist, 50.5) == 2  # rank 51
    assert analysis.percentile(hist, 95) == 2
    assert analysis.percentile(hist, 96) == 10
    assert analysis.percentile(hist, 99) == 10
    assert analysis.percentile(hist, 99.9) == 100  # rank ceil(99.9) = 100
    assert analysis.percentile(hist, 100) == 100
    # One sample: every percentile is that sample.
    assert analysis.percentile({7: 1}, 1) == 7


def test_percentile_rejects_empty_input_and_bad_q() -> None:
    with pytest.raises(ValueError):
        analysis.percentile({}, 50)
    with pytest.raises(ValueError):
        analysis.percentile({1: 1}, 0)
    with pytest.raises(ValueError):
        analysis.percentile({1: 1}, 101)


def test_summarize() -> None:
    s = analysis.summarize({1: 3, 5: 1})
    assert s == {
        "count": 4,
        "mean": 2.0,
        "max": 5,
        "p50": 1,
        "p95": 5,
        "p99": 5,
        "p99.9": 5,
    }
    assert analysis.summarize({}) == {"count": 0}


def test_histogram_keys_by_upper_edge_so_percentiles_never_flatter() -> None:
    h = analysis.histogram([0.31, 0.3, 0.01, 0.1, 1.05], 0.1)
    # 0.3 stays in 0.3 (not 0.4, despite 0.3/0.1 = 2.9999999999999996); 0.31 rounds up.
    assert h == {0.1: 2, 0.3: 1, 0.4: 1, 1.1: 1}
    assert analysis.histogram([], 0.1) == {}


def test_merge_adds_counts() -> None:
    assert analysis.merge([{1: 2, 3: 1}, {1: 1, 2: 5}]) == {1: 3, 2: 5, 3: 1}


def test_window_is_half_open_and_rate_counts_only_inside() -> None:
    w = Window.of_run(t0_ms=10_000, warmup_s=2, measure_s=3)
    assert (w.start_ms, w.end_ms, w.seconds) == (12_000, 15_000, 3.0)
    assert 12_000 in w
    assert 14_999 in w
    assert 15_000 not in w
    assert 11_999 not in w
    times = [11_999, 12_000, 13_000, 14_999, 15_000, 20_000]
    assert analysis.rate_in(w, times) == 1.0  # 3 events in 3 s


def test_value_at_interpolates_between_samples_and_clamps_outside() -> None:
    samples = [(0.0, 0.0), (10.0, 5.0), (20.0, 5.0)]
    assert analysis.value_at(samples, 5.0) == 2.5
    assert analysis.value_at(samples, 15.0) == 5.0
    assert analysis.value_at(samples, -1.0) == 0.0
    assert analysis.value_at(samples, 99.0) == 5.0
    with pytest.raises(ValueError):
        analysis.value_at([], 1.0)


def test_busy_fraction_uses_only_the_window() -> None:
    # One core fully busy from t=1 s to t=3 s, idle otherwise (cumulative CPU seconds).
    samples = [(0.0, 0.0), (1000.0, 0.0), (3000.0, 2.0), (6000.0, 2.0)]
    assert analysis.busy_fraction(samples, Window(1000, 3000)) == 1.0
    assert analysis.busy_fraction(samples, Window(0, 4000)) == 0.5
    assert analysis.busy_fraction(samples, Window(2000, 3000)) == 1.0


def test_busy_fraction_refuses_to_extrapolate() -> None:
    samples = [(1000.0, 0.0), (3000.0, 2.0)]
    assert analysis.busy_fraction(samples, Window(0, 2000)) is None  # starts before
    assert analysis.busy_fraction(samples, Window(2000, 4000)) is None  # ends after
    assert analysis.busy_fraction(samples[:1], Window(1000, 1000)) is None


def test_payload_is_padded_to_the_requested_json_size() -> None:
    spec = LoadSpec(redis_url="", payload={"latency_ms": 5}, payload_bytes=300)
    p = make_payload(spec)
    assert len(json.dumps(p, separators=(",", ":"))) == 300
    assert p["latency_ms"] == 5
    # Fields are never truncated to fit: a small target just gets no padding.
    assert make_payload(LoadSpec(redis_url="", payload_bytes=1))["pad"] == ""


def test_command_costs_are_deltas_per_job_sorted_by_time() -> None:
    before = {"cmdstat_xadd": {"calls": 10, "usec": 100}, "cmdstat_ping": {"calls": 5, "usec": 5}}
    after = {
        "cmdstat_xadd": {"calls": 30, "usec": 300, "usec_per_call": 10.0},
        "cmdstat_ping": {"calls": 5, "usec": 5},  # unchanged: not in the result
        "cmdstat_evalsha": {"calls": 20, "usec": 800},  # new since `before`
    }
    costs = command_costs(before, after, jobs=10)
    assert list(costs) == ["evalsha", "xadd"]
    assert costs["xadd"] == {"calls": 20, "usec": 200, "calls_per_job": 2.0, "usec_per_job": 20.0}
    assert costs["evalsha"]["usec_per_job"] == 80.0
    assert command_costs(before, after, jobs=0)["xadd"]["usec_per_job"] == 0.0
