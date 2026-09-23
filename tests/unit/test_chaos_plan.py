"""The chaos fault plan (chaos/faults.py): pure logic, so plain unit tests."""

import asyncio
import random
from collections import Counter

import pytest

from chaos import faults
from chaos.topology import Result
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


# ---------------------------------------------------------------- the injector
# CI run 35854442425 failed I4 with kills = 2 < 3: two of four planned kills were aimed
# at a worker a crashy job had just killed, and `docker kill` of a stopped container
# fails. The injector now waits for the supervisor to restart the worker and retries.
# These tests fake `docker` (pure injector logic; the real thing runs in every chaos run).


def _fake_docker(monkeypatch: pytest.MonkeyPatch, down_for: int) -> list[tuple[str, ...]]:
    """`docker kill`/`pause` fail as "not running" for the first `down_for` attempts."""
    calls: list[tuple[str, ...]] = []
    left = {"n": down_for}

    async def fake_run(*args: str, **_: object) -> Result:
        calls.append(args)
        if args[1] in ("kill", "pause") and left["n"] > 0:
            left["n"] -= 1
            raise RuntimeError(f"docker {args[1]} exited 1: container is not running")
        return Result(0, "", "")

    monkeypatch.setattr(faults, "run", fake_run)
    return calls


def _injector(retry_for: float) -> faults.Injector:
    inj = faults.Injector(stack=None, toxi=None)  # type: ignore[arg-type]  # not used here
    inj.retry_for = retry_for
    inj.retry_interval = 0.01
    return inj


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["kill", "pause"])
async def test_a_fault_aimed_at_a_worker_that_is_down_waits_for_it_and_happens(
    monkeypatch: pytest.MonkeyPatch, kind: faults.FaultKind
) -> None:
    calls = _fake_docker(monkeypatch, down_for=3)
    inj = _injector(retry_for=5.0)
    await inj.run([faults.Fault(0.0, kind, 1, 0.0)], asyncio.get_running_loop().time())
    assert inj.skipped == []
    assert [f["kind"] for f in inj.executed] == [kind]
    assert inj.executed[0]["attempts"] == 4
    assert [c[1] for c in calls].count(kind) == 4
    assert inj.held_down == set()  # a kill's hold ends when the worker is started again


@pytest.mark.asyncio
async def test_a_fault_whose_worker_stays_down_is_skipped_and_releases_the_hold(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _fake_docker(monkeypatch, down_for=10_000)
    inj = _injector(retry_for=0.1)
    await inj.run([faults.Fault(0.0, "kill", 1, 0.0)], asyncio.get_running_loop().time())
    assert inj.executed == []
    assert len(inj.skipped) == 1
    assert inj.skipped[0]["attempts"] > 1
    # Held down between attempts, the supervisor could never restart the worker.
    assert inj.held_down == set()
