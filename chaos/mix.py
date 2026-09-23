"""The chaos job mix: which kinds of jobs a run enqueues, and how each one must end.

Every kind has a fixed expectation the verifier checks (invariants I1-I3): its terminal
state, whether it performs an effect (and under which ledger key), and for the kinds
that must die, why. Anything nondeterministic about a job (how many times a flaky job
fails, how long a slow job sleeps) is drawn from the run's seeded RNG *at enqueue time*
and written into its payload, so the handler's behaviour is fixed before it runs and a
seed reproduces the whole mix.

Flaky jobs fail a fixed k times (1 <= k <= 3 < max_attempts); a random failure rate would
sometimes exhaust max_attempts by bad luck and create false DEADs at scale (SPEC §7).
SPEC suggests deriving k from a seeded hash of the job_id; job_ids are made by the client
inside enqueue, so k comes from the seeded RNG instead. Same property: deterministic per
job and per seed.
"""

import random
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, Literal

from ftq.client import NewJob

Terminal = Literal["SUCCEEDED", "DEAD"]


@dataclass(frozen=True)
class MixParams:
    """Worker settings the mix has to agree with."""

    lease: float  # visibility_timeout (s)
    max_attempts: int
    max_deliveries: int


@dataclass(frozen=True)
class Kind:
    name: str
    job_type: str  # the handler (ftq.handlers)
    share: float  # fraction of the run's jobs
    minimum: int  # at least this many, so even a small run has every kind
    expect: Terminal
    effect: str | None  # ledger key prefix: the effect key is f"{effect}:{job_id}"
    payload: Callable[[random.Random, MixParams], dict[str, Any]]
    dlq_reason: str | None = None  # for kinds that must end DEAD


def _none(_rng: random.Random, _p: MixParams) -> dict[str, Any]:
    return {}


KINDS: tuple[Kind, ...] = (
    # Plain I/O jobs with a little simulated latency, so a kill or pause catches some of
    # them mid-run. They make up everything the other kinds don't.
    Kind(
        "normal",
        "send_email",
        0.0,
        0,
        "SUCCEEDED",
        "send_email",
        lambda rng, _p: {"latency_ms": rng.randint(0, 20)},
    ),
    Kind(
        "flaky",
        "flaky",
        0.03,
        5,
        "SUCCEEDED",
        "flaky",
        lambda rng, _p: {"fail_times": rng.randint(1, 3)},
    ),
    # No heartbeats and longer than the lease: guaranteed to be reclaimed while the first
    # holder still runs, so real suppressed duplicates happen (ADR-007).
    Kind(
        "slow",
        "slow",
        0.003,
        5,
        "SUCCEEDED",
        "slow",
        lambda rng, p: {"seconds": round(rng.uniform(1.2, 1.8) * p.lease, 3)},
    ),
    # CPU jobs in the process pool: the bystanders a hang_process timeout's pool reset
    # restarts (ADR-030). No effect (sync handlers have no ledger).
    Kind("cpu", "cpu_task", 0.02, 10, "SUCCEEDED", None, lambda _rng, _p: {"rounds": 2000}),
    # Timeouts under chaos (ADR-036): each hangs on its first attempt or two, times out
    # (HANG_TIMEOUT), and then succeeds. One kind per way a run is stopped.
    Kind(
        "hang",
        "hang",
        0.001,
        3,
        "SUCCEEDED",
        "hang",
        lambda rng, _p: {"hang_attempts": rng.randint(1, 2)},
    ),
    Kind(
        "hang_thread",
        "hang_thread",
        0.0005,
        3,
        "SUCCEEDED",
        None,
        # The orphaned thread returns a few seconds after its timeout, so orphans come
        # and go instead of eating a worker's slots for good.
        lambda rng, _p: {"hang_attempts": 1, "hang_seconds": round(rng.uniform(3, 5), 3)},
    ),
    Kind(
        "hang_process",
        "hang_process",
        0.001,
        3,
        "SUCCEEDED",
        None,
        lambda rng, _p: {"hang_attempts": rng.randint(1, 2)},
    ),
    # Hangs on every attempt: max_attempts timeouts (each one a pool reset), then DEAD.
    Kind(
        "hang_forever",
        "hang_process",
        0.00003,
        2,
        "DEAD",
        None,
        lambda _rng, p: {"hang_attempts": p.max_attempts + 1000},
        dlq_reason="max_attempts",
    ),
    Kind("poison", "poison", 0.0002, 3, "DEAD", None, _none, dlq_reason="max_attempts"),
    # Kills its worker process on every delivery until max_deliveries sends it to the
    # DLQ unrun. Kept rare: every one of them costs max_deliveries worker crashes.
    Kind("crashy", "crashy", 0.00003, 3, "DEAD", None, _none, dlq_reason="max_deliveries"),
)
BY_NAME = {k.name: k for k in KINDS}


def counts(n: int) -> dict[str, int]:
    """How many jobs of each kind a run of `n` jobs has. `normal` fills the rest."""
    special = {k.name: max(k.minimum, round(n * k.share)) for k in KINDS if k.name != "normal"}
    if sum(special.values()) >= n:
        raise ValueError(f"{n} jobs is too few for the mix's minimums ({sum(special.values())})")
    return {"normal": n - sum(special.values()), **special}


def build(n: int, rng: random.Random, params: MixParams) -> list[tuple[str, NewJob]]:
    """The run's jobs in enqueue order: (kind name, job), shuffled so every kind is spread
    over the whole run instead of arriving in one block."""
    names = [name for name, c in counts(n).items() for _ in range(c)]
    rng.shuffle(names)
    return [
        (name, NewJob(BY_NAME[name].job_type, BY_NAME[name].payload(rng, params))) for name in names
    ]
