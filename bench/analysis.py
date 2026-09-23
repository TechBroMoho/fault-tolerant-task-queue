"""The benchmark's arithmetic: steady-state windows, rates, percentiles, CPU fractions.

Pure functions with no I/O, so they are unit-tested directly
(tests/unit/test_bench_analysis.py) and shared by the load generator, the local driver,
and the charts. Two conventions hold throughout:

- **Times are milliseconds on one clock**, Redis `TIME` (ADR-020). Enqueue and completion
  are both stamped inside Lua scripts, so a latency is the difference of two readings of
  one clock, whichever machine the producer and worker ran on.
- **Percentiles are nearest-rank** (the smallest value with at least q% of the samples at
  or below it). It never interpolates a latency nobody observed, and it can be
  recomputed exactly from the histograms the reports store.
"""

import math
from collections import Counter
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from itertools import pairwise

PERCENTILES = (50.0, 95.0, 99.0, 99.9)


@dataclass(frozen=True, slots=True)
class Window:
    """The steady-state part of a run, [start_ms, end_ms), on Redis's clock.

    A run produces load for warmup + measure + cooldown seconds. Only the middle part
    counts: the warmup lets workers, connections, and the backlog settle, and the
    cooldown keeps the producers' stop out of the window (SPEC Phase 6).
    """

    start_ms: int
    end_ms: int

    @classmethod
    def of_run(cls, t0_ms: int, warmup_s: float, measure_s: float) -> "Window":
        start = t0_ms + round(warmup_s * 1000)
        return cls(start, start + round(measure_s * 1000))

    @property
    def seconds(self) -> float:
        return (self.end_ms - self.start_ms) / 1000

    def __contains__(self, t_ms: object) -> bool:
        return isinstance(t_ms, int | float) and self.start_ms <= t_ms < self.end_ms


def rate_in(window: Window, times_ms: Iterable[int]) -> float:
    """Events per second whose timestamp falls inside the window."""
    return sum(1 for t in times_ms if t in window) / window.seconds


def percentile[N: (int, float)](hist: Mapping[N, int], q: float) -> N:
    """Nearest-rank q-th percentile (0 < q <= 100) of a histogram {value: count}."""
    total = sum(hist.values())
    if total == 0:
        raise ValueError("percentile of an empty histogram")
    if not 0 < q <= 100:
        raise ValueError(f"q must be in (0, 100], got {q}")
    rank = math.ceil(q / 100 * total)  # 1-based rank of the answer
    seen = 0
    for value in sorted(hist):
        seen += hist[value]
        if seen >= rank:
            return value
    raise AssertionError("unreachable: ranks are bounded by the total")


def summarize[N: (int, float)](hist: Mapping[N, int]) -> dict[str, float]:
    """count, mean, max, and the standard percentiles of a histogram; {"count": 0} if
    it's empty (a run can legitimately have no samples in a window, e.g. no rejections)."""
    total = sum(hist.values())
    if total == 0:
        return {"count": 0}
    out: dict[str, float] = {
        "count": total,
        "mean": round(sum(v * n for v, n in hist.items()) / total, 3),
        "max": max(hist),
    }
    for q in PERCENTILES:
        out[f"p{q:g}"] = percentile(hist, q)
    return out


def histogram(values: Iterable[float], bucket: float) -> dict[float, int]:
    """Count values into buckets of width `bucket`, keyed by each bucket's upper edge.

    Keying by the upper edge makes every percentile read from the histogram an upper
    bound on the true one (a 0.31 ms call lands in the 0.4 ms bucket at bucket 0.1), so
    the bucketing can never make latency look better than it was.
    """
    counts: Counter[float] = Counter()
    for v in values:
        edge = math.ceil(v / bucket - 1e-9) * bucket  # 1e-9: 0.3/0.1 is 2.9999999999999996
        counts[round(edge, 6)] += 1
    return dict(sorted(counts.items()))


def merge[N: (int, float)](hists: Iterable[Mapping[N, int]]) -> dict[N, int]:
    total: Counter[N] = Counter()
    for h in hists:
        total.update(h)
    return dict(sorted(total.items()))


def value_at(samples: Sequence[tuple[float, float]], t: float) -> float:
    """Linear interpolation of a cumulative counter sampled as (time, value) pairs.

    Used for CPU seconds, which only ever grow: the CPU a process spent inside a window
    is value_at(end) - value_at(start), however the samples fall around the edges.
    """
    if not samples:
        raise ValueError("no samples")
    if t <= samples[0][0]:
        return samples[0][1]
    for (t0, v0), (t1, v1) in pairwise(samples):
        if t0 <= t <= t1:
            return v0 if t1 == t0 else v0 + (v1 - v0) * (t - t0) / (t1 - t0)
    return samples[-1][1]


def busy_fraction(samples: Sequence[tuple[float, float]], window: Window) -> float | None:
    """CPU seconds per wall second inside the window, from cumulative (t_ms, cpu_s)
    samples. 1.0 = one core fully busy. None if the samples don't cover the window
    (extrapolating would invent a number)."""
    if len(samples) < 2 or samples[0][0] > window.start_ms or samples[-1][0] < window.end_ms:
        return None
    used = value_at(samples, window.end_ms) - value_at(samples, window.start_ms)
    return round(used / window.seconds, 3)
