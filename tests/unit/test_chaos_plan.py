"""The chaos fault plan (chaos/faults.py): pure logic, so plain unit tests."""

import random
from collections import Counter

from chaos import faults
from chaos.verifier import Minimums


def test_every_kind_is_planned_often_enough_for_i4_even_in_a_short_run() -> None:
    # A 100K run once drew only 2 pauses and failed I4 (PROGRESS.md, Phase 4). The
    # opening rounds now guarantee each kind, stretching a short run's plan to fit, with
    # one to spare over I4's minimums in case a fault is skipped.
    need = Minimums()
    for seed in range(500):
        plan = faults.plan(random.Random(seed), workers=8, lease=2.0, span=10.0)
        counts: Counter[str] = Counter(f.kind for f in plan)
        assert counts["kill"] > need.kills, (seed, counts)
        assert counts["pause"] > need.pauses, (seed, counts)
        assert sum(counts[k] for k in faults.NETWORK) > need.network_windows, (seed, counts)
        assert all(counts[k] >= 2 for k in faults.NETWORK), (seed, counts)


def test_one_fault_per_worker_at_a_time_and_at_most_half_the_workers() -> None:
    for seed in range(200):
        plan = faults.plan(random.Random(seed), workers=8, lease=2.0, span=70.0)
        for f in plan:
            overlapping = [g for g in plan if g is not f and g.at <= f.at < g.at + g.duration]
            assert all(g.worker != f.worker for g in overlapping), (seed, f)
            assert len(overlapping) < 4, (seed, f)  # f itself makes at most 8 // 2


def test_pauses_outlast_the_lease_and_the_plan_is_reproducible() -> None:
    plan = faults.plan(random.Random(7), workers=8, lease=2.0, span=70.0)
    assert all(f.duration > 2.0 for f in plan if f.kind == "pause")
    assert plan == faults.plan(random.Random(7), workers=8, lease=2.0, span=70.0)
