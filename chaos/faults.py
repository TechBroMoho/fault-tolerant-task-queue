"""The fault schedule, the injector that carries it out, and the supervisor that restarts
crashed workers.

The schedule is generated up front from the run's seed, so a seed reproduces *what* was
planned. What actually happened is recorded separately (`Injector.executed` / `skipped`)
because a planned fault can legitimately not happen (you can't pause a worker that a
crashy job just killed), and invariant I4 counts only faults that happened.

Fault kinds (SPEC §7):
- kill: `docker kill -s KILL` a worker, keep it down a moment, then start it again. Its
  in-flight jobs are abandoned mid-run; their leases expire and other workers reclaim them.
- pause: `docker pause` for longer than the lease, then unpause: the GC-pause "zombie".
  While it's frozen, others reclaim its jobs; when it wakes, it finishes and commits them
  late. That is what produces real suppressed duplicates.
- reset_peer / timeout / latency: a Toxiproxy toxic on that worker's proxy only.
  `timeout` drops replies (downstream) while commands still reach Redis: lost replies.
- partition: that worker's proxy is disabled (connections closed and refused).
"""

import asyncio
import contextlib
import logging
import random
from collections import Counter
from dataclasses import asdict, dataclass
from typing import Any, Literal

from chaos.topology import PROJECT, Stack, proxy_name, run, worker_name
from chaos.toxiproxy import Toxiproxy

log = logging.getLogger("chaos")

FaultKind = Literal["kill", "pause", "reset_peer", "timeout", "latency", "partition"]
KINDS: tuple[FaultKind, ...] = ("kill", "pause", "reset_peer", "timeout", "latency", "partition")
NETWORK: frozenset[str] = frozenset({"reset_peer", "timeout", "latency", "partition"})
# Relative frequency after the opening rounds (see `plan`).
_WEIGHTS = {
    "kill": 2.0,
    "pause": 2.0,
    "reset_peer": 1.5,
    "timeout": 1.5,
    "latency": 1.0,
    "partition": 1.5,
}
# Every kind is planned at least this often (see `plan`); I4 needs 3 kills and 3 pauses.
OPENING_ROUNDS = 4
# After a fault is healed, leave its worker alone this long (a killed worker needs a
# moment to restart) before it can be picked again.
_SETTLE_S = 2.0


@dataclass(frozen=True)
class Fault:
    at: float  # seconds after the fault phase starts
    kind: FaultKind
    worker: int  # 1-based
    duration: float  # how long the fault lasts (kill: how long the worker stays down)
    latency_ms: int = 0  # latency only


def _duration(kind: FaultKind, rng: random.Random, lease: float) -> tuple[float, int]:
    if kind == "kill":
        return rng.uniform(0.5, 3.0), 0
    if kind == "pause":
        # Longer than the lease, so the frozen worker's jobs really are reclaimed.
        return rng.uniform(1.5 * lease, 3.0 * lease), 0
    if kind == "latency":
        return rng.uniform(2.0, 5.0), rng.randint(50, 250)
    return rng.uniform(1.0, 4.0), 0  # reset_peer, timeout, partition


def plan(
    rng: random.Random,
    workers: int,
    lease: float,
    span: float,
    gap: tuple[float, float] = (1.0, 3.0),
) -> list[Fault]:
    """Faults over at least `span` seconds, one every `gap` seconds on average.

    The opening rounds go through every kind OPENING_ROUNDS times (in a shuffled order),
    and the plan runs past `span` if that's what it takes to fit them. So every run has
    at least that many kills, pauses, and network faults planned, comfortably above I4's
    minimums even if one is skipped. After the opening, kinds are drawn by `_WEIGHTS`.
    (Drawing every fault at random once left a 100K run with 2 pauses, and I4 failed.)
    A worker has at most one fault at a time, and at most half the workers are faulted
    at once, so the run keeps making progress.
    """
    opening = [k for k in KINDS for _ in range(OPENING_ROUNDS)]
    rng.shuffle(opening)
    busy_until = [0.0] * (workers + 1)
    max_active = max(1, workers // 2)
    faults: list[Fault] = []
    t = 2.0  # let the workers start first
    while t < span or opening:
        kind = opening[0] if opening else rng.choices(KINDS, [_WEIGHTS[k] for k in KINDS])[0]
        free = [w for w in range(1, workers + 1) if busy_until[w] <= t]
        active = workers - len(free)
        if free and active < max_active:
            w = rng.choice(free)
            duration, latency_ms = _duration(kind, rng, lease)
            faults.append(Fault(round(t, 3), kind, w, round(duration, 3), latency_ms))
            busy_until[w] = t + duration + _SETTLE_S
            if opening:
                opening.pop(0)
        t += rng.uniform(*gap)
    return faults


class Injector:
    """Carries out a schedule. Each fault runs as its own task: apply, wait, heal."""

    def __init__(self, stack: Stack, toxi: Toxiproxy) -> None:
        self._stack = stack
        self._toxi = toxi
        self.held_down: set[str] = set()  # killed on purpose: the supervisor leaves these
        self.executed: list[dict[str, Any]] = []
        self.skipped: list[dict[str, Any]] = []

    async def run(self, faults: list[Fault], t0: float) -> None:
        await asyncio.gather(*(self._one(f, t0) for f in faults))

    async def _one(self, f: Fault, t0: float) -> None:
        loop = asyncio.get_running_loop()
        await asyncio.sleep(max(0.0, t0 + f.at - loop.time()))
        started = loop.time() - t0
        try:
            await self._apply(f)
        except Exception as exc:  # e.g. pausing a worker a crashy job just killed
            self.skipped.append({**asdict(f), "reason": str(exc)[:300]})
            log.info("fault skipped: %s worker-%d (%s)", f.kind, f.worker, exc)
            return
        log.info("fault: %s worker-%d for %.1fs", f.kind, f.worker, f.duration)
        await asyncio.sleep(f.duration)
        await self._heal(f)
        self.executed.append(
            {**asdict(f), "started": round(started, 3), "healed": round(loop.time() - t0, 3)}
        )

    async def _apply(self, f: Fault) -> None:
        name, proxy = worker_name(f.worker), proxy_name(f.worker)
        if f.kind == "kill":
            self.held_down.add(name)
            try:
                await run("docker", "kill", "-s", "KILL", name)
            except Exception:
                self.held_down.discard(name)
                raise
        elif f.kind == "pause":
            await run("docker", "pause", name)
        elif f.kind == "partition":
            await self._toxi.set_enabled(proxy, False)
        elif f.kind == "latency":
            await self._toxi.add_toxic(proxy, "latency", {"latency": f.latency_ms, "jitter": 50})
        elif f.kind == "timeout":
            await self._toxi.add_toxic(proxy, "timeout", {"timeout": 0})  # 0: drop until removed
        else:
            await self._toxi.add_toxic(proxy, "reset_peer", {"timeout": 0})

    async def _heal(self, f: Fault) -> None:
        name, proxy = worker_name(f.worker), proxy_name(f.worker)
        if f.kind == "kill":
            await run("docker", "start", name)
            self.held_down.discard(name)
        elif f.kind == "pause":
            await run("docker", "unpause", name)
        elif f.kind == "partition":
            await self._toxi.set_enabled(proxy, True)
        else:
            await self._toxi.remove_toxic(proxy)

    def counts(self) -> dict[str, int]:
        c = Counter(str(f["kind"]) for f in self.executed)
        return {
            "kills": c["kill"],
            "pauses": c["pause"],
            "network_windows": sum(c[k] for k in NETWORK),
            **{f"by_kind.{k}": c[k] for k in KINDS},
            "skipped": len(self.skipped),
        }


async def heal_all(stack: Stack, toxi: Toxiproxy) -> None:
    """End of the fault phase: remove every toxic, enable every proxy, unpause every
    worker. (Exited workers are the supervisor's job.)"""
    await toxi.reset()
    paused = await run(
        "docker",
        "ps",
        "--filter",
        f"name=^{PROJECT}-worker-",
        "--filter",
        "status=paused",
        "--format",
        "{{.Names}}",
    )
    for name in paused.out.split():
        await run("docker", "unpause", name)


class Supervisor:
    """Restarts worker containers that exited on their own, like ECS or Kubernetes would.

    A crashy job exits its worker with code 70; anything else unexpected shows up in
    `restarts` under its own exit code. Workers the injector killed on purpose are left
    down until the injector restarts them.
    """

    def __init__(self, stack: Stack, injector: Injector) -> None:
        self._stack = stack
        self._injector = injector
        self.restarts: Counter[int] = Counter()

    async def run(self, stop: asyncio.Event, interval: float = 0.5) -> None:
        while not stop.is_set():
            # A worker the injector is holding down (or just restarted) must not be counted
            # or restarted here: check the hold both before and after reading the states.
            held_before = set(self._injector.held_down)
            for name, (state, code) in (await self._stack.worker_states()).items():
                held = name in held_before or name in self._injector.held_down
                if state == "exited" and not held:
                    await run("docker", "start", name)
                    self.restarts[code if code is not None else -1] += 1
                    log.info("supervisor: restarted %s (exit code %s)", name, code)
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(stop.wait(), interval)
