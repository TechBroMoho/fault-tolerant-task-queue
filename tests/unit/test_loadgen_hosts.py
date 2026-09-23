"""A producer host's summary crosses to the coordinator as JSON through Redis. JSON turns
int and float dict keys into strings, which would split histogram buckets (0.4 and
"0.4" are different keys) or crash the sorted merge. The round trip must be exact."""

from bench.loadgen import summary_from_json, summary_to_json, to_local_epoch


def test_a_producer_summary_survives_the_trip_through_redis() -> None:
    summary = {
        "index": 0,
        "pid": 7,
        "offered": 10,
        "accepted": 9,
        "cpu_busy": 0.25,
        "per_second": {0: [5, 5, 0], 1: [5, 4, 1]},
        "enqueue_call_hist_ms": {0.4: 3, 1.2: 1},
        "batch_sizes": {5: 2},
    }
    assert summary_from_json(summary_to_json([summary])) == [summary]


def test_the_start_time_is_converted_with_the_hosts_offset_to_redis() -> None:
    """Hosts agree on Redis TIME, not on their own clocks: a host whose clock is 250 ms
    ahead of Redis must start 250 ms later by its own clock."""
    assert to_local_epoch(start_redis_ms=10_000, offset_ms=-250) == 10.25
    assert to_local_epoch(start_redis_ms=10_000, offset_ms=0) == 10.0
