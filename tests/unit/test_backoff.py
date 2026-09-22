"""Retry backoff: full jitter stays within its bounds (SPEC §7 Phase 2, pure logic)."""

import random

import pytest

from ftq.backoff import backoff_bound, full_jitter_delay


def test_bound_doubles_then_caps() -> None:
    bounds = [backoff_bound(a, base=1.0, cap=10.0) for a in range(6)]
    assert bounds == [1.0, 2.0, 4.0, 8.0, 10.0, 10.0]


def test_huge_attempt_stays_finite() -> None:
    assert backoff_bound(10_000, base=0.5, cap=300.0) == 300.0


def test_negative_attempt_rejected() -> None:
    with pytest.raises(ValueError):
        backoff_bound(-1, base=1.0, cap=10.0)


@pytest.mark.parametrize("seed", range(5))
def test_every_delay_is_within_zero_and_its_bound(seed: int) -> None:
    rng = random.Random(seed)
    for attempt in range(12):
        bound = backoff_bound(attempt, base=0.1, cap=5.0)
        for _ in range(500):
            assert 0.0 <= full_jitter_delay(attempt, 0.1, 5.0, rng) <= bound


def test_jitter_spreads_over_the_whole_window() -> None:
    """Full jitter, not "equal jitter": delays cover [0, bound], not just its top half.
    Without that, simultaneous failures still retry in a wave."""
    rng = random.Random(42)
    delays = [full_jitter_delay(3, 1.0, 100.0, rng) for _ in range(4000)]  # bound = 8 s
    assert min(delays) < 0.5 and max(delays) > 7.5
    assert 3.6 < sum(delays) / len(delays) < 4.4  # uniform mean = bound / 2


def test_seeded_rng_is_reproducible() -> None:
    a = [full_jitter_delay(2, 1.0, 10.0, random.Random(7)) for _ in range(3)]
    b = [full_jitter_delay(2, 1.0, 10.0, random.Random(7)) for _ in range(3)]
    assert a == b
