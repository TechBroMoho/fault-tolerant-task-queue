"""The chaos verifier: invariants I1-I5 (SPEC §7), checked against what Redis recorded.

The evidence is append-only wherever duplicates are the question (SPEC §4):
- the **effects log**, one entry per effect the ledger let through (ledger.lua). A
  `SET NX` marker could never show a duplicate; a log can.
- the **results log**, one entry per first commit (commit.lua).
Terminal states come from each job's done hash, and DLQ entries from the DLQ stream.

The verifier knows nothing about how the run went. It gets the accepted jobs (job_id ->
kind), the kinds' expectations (mix.py), and, for I4, what the orchestrator says it did.
The mutation tests call it on small in-process scenarios with I4 left out
(tests/integration/test_chaos_verifier.py).
"""

from collections import Counter
from dataclasses import dataclass, field
from typing import Any

import redis.asyncio as aioredis

from chaos.mix import BY_NAME
from ftq.keys import Keys
from ftq.metrics import read_counters

_SAMPLE = 20  # violations listed per invariant; the counts are always complete
_PAGE = 10_000  # XRANGE page size
_PIPE = 5_000  # done-key reads per pipelined round trip


@dataclass
class Check:
    ok: bool
    detail: dict[str, Any] = field(default_factory=dict)
    violations: list[str] = field(default_factory=list)

    def fail(self, message: str) -> None:
        self.ok = False
        if len(self.violations) < _SAMPLE:
            self.violations.append(message)

    def as_dict(self) -> dict[str, Any]:
        return {"ok": self.ok, **self.detail, "violations": self.violations}


@dataclass(frozen=True)
class Minimums:
    """I4: what must have happened for the run to prove anything. Never relax these to
    make a run pass; fix the fault schedule instead (SPEC §7)."""

    kills: int = 3
    pauses: int = 3
    network_windows: int = 6
    reclaimed: int = 1
    duplicates_suppressed: int = 1
    timeouts: int = 1
    pool_resets: int = 1
    crash_restarts: int = 1


async def _stream(redis: aioredis.Redis, key: str) -> list[tuple[str, dict[str, str]]]:
    out: list[tuple[str, dict[str, str]]] = []
    start = "-"
    while True:
        page: Any = await redis.xrange(key, min=start, count=_PAGE)
        out.extend(page)
        if len(page) < _PAGE:
            return out
        start = f"({page[-1][0]}"


async def _states(redis: aioredis.Redis, keys: Keys, job_ids: list[str]) -> dict[str, str]:
    states: dict[str, str] = {}
    for i in range(0, len(job_ids), _PIPE):
        chunk = job_ids[i : i + _PIPE]
        pipe = redis.pipeline(transaction=False)
        for job_id in chunk:
            pipe.hget(keys.done(job_id), "state")
        for job_id, state in zip(chunk, await pipe.execute(), strict=True):
            states[job_id] = state or ""
    return states


async def verify(
    redis: aioredis.Redis,
    keys: Keys,
    group: str,
    accepted: dict[str, str],
    max_attempts: int,
    max_deliveries: int,
    evidence: dict[str, int] | None = None,
    minimums: Minimums | None = None,
) -> dict[str, Any]:
    """Check every invariant; return the report section. `accepted`: job_id -> kind name.
    `evidence` (fault and restart counts from the orchestrator) enables I4."""
    job_ids = list(accepted)
    states = await _states(redis, keys, job_ids)
    results = Counter(f["job_id"] for _id, f in await _stream(redis, keys.results))
    effects = Counter(f["key"] for _id, f in await _stream(redis, keys.effects))
    dlq = await _stream(redis, keys.dead)
    counters = await read_counters(redis, keys)

    # ---- I1: every accepted job has exactly one terminal state, the one its kind must
    # end in; DEAD only for the kinds meant to die (poison, crashy, hang_forever).
    i1 = Check(True)
    by_state: Counter[str] = Counter()
    for job_id, kind in accepted.items():
        state = states[job_id]
        by_state[state or "NONE"] += 1
        if state not in ("SUCCEEDED", "DEAD"):
            i1.fail(f"{job_id} ({kind}): no terminal state")
        elif state != BY_NAME[kind].expect:
            i1.fail(f"{job_id} ({kind}): {state}, expected {BY_NAME[kind].expect}")
    # A late success means some job was DEAD at some point and then succeeded. None of
    # the kinds that may die can ever succeed, so that job was falsely DEAD for a while.
    if counters["late_successes"]:
        i1.fail(f"late_successes = {counters['late_successes']}: a job was transiently DEAD")
    i1.detail = {"jobs": len(accepted), "states": dict(by_state)}

    # ---- I2: in the append-only effects log, each SUCCEEDED job's effect key appears
    # exactly once; jobs that died, and kinds without effects, appear zero times.
    i2 = Check(True)
    expected_keys: set[str] = set()
    for job_id, kind in accepted.items():
        prefix = BY_NAME[kind].effect
        if prefix is None:
            continue
        key = f"{prefix}:{job_id}"
        expected_keys.add(key)
        want = 1 if states[job_id] == "SUCCEEDED" else 0
        if effects[key] != want:
            i2.fail(f"{key} ({kind}, {states[job_id] or 'no state'}): {effects[key]} effects")
    stray = [k for k in effects if k not in expected_keys]
    for key in stray:
        i2.fail(f"effect {key} ({effects[key]}x) belongs to no accepted job with effects")
    i2.detail = {
        "effects_logged": sum(effects.values()),
        "distinct_keys": len(effects),
        "duplicated_keys": sum(1 for n in effects.values() if n > 1),
    }

    # ---- I2b: in the append-only results log, each SUCCEEDED job_id appears exactly
    # once, and no other job_id appears at all.
    i2b = Check(True)
    for job_id, kind in accepted.items():
        want = 1 if states[job_id] == "SUCCEEDED" else 0
        if results[job_id] != want:
            i2b.fail(
                f"{job_id} ({kind}, {states[job_id] or 'no state'}): {results[job_id]} results"
            )
    for job_id in results.keys() - accepted.keys():
        i2b.fail(f"result for {job_id}, which was never accepted")
    i2b.detail = {
        "results_logged": sum(results.values()),
        "duplicated_job_ids": sum(1 for n in results.values() if n > 1),
    }

    # ---- I3: every job meant to die is in the DLQ exactly once, with the right reason
    # and counts, and the DLQ holds nothing else.
    i3 = Check(True)
    dlq_by_job: dict[str, list[dict[str, str]]] = {}
    for _id, f in dlq:
        dlq_by_job.setdefault(f.get("dlq_job_id", "?"), []).append(f)
    for job_id, kind in accepted.items():
        k = BY_NAME[kind]
        entries = dlq_by_job.get(job_id, [])
        if k.expect != "DEAD":
            if entries:
                i3.fail(f"{job_id} ({kind}) is in the DLQ ({entries[0].get('dlq_reason')})")
            continue
        if len(entries) != 1:
            i3.fail(f"{job_id} ({kind}): {len(entries)} DLQ entries")
            continue
        e = entries[0]
        if e["dlq_reason"] != k.dlq_reason:
            i3.fail(f"{job_id} ({kind}): reason {e['dlq_reason']}, expected {k.dlq_reason}")
        elif kind == "crashy" and e["dlq_deliveries"] != str(max_deliveries + 1):
            i3.fail(f"{job_id} (crashy): dead-lettered at delivery {e['dlq_deliveries']}")
        elif kind in ("poison", "hang_forever") and e["dlq_attempts"] != str(max_attempts):
            i3.fail(f"{job_id} ({kind}): dead after {e['dlq_attempts']} attempts")
        elif kind == "hang_forever" and "HandlerTimeout" not in e["dlq_error"]:
            i3.fail(f"{job_id} (hang_forever): last error {e['dlq_error'][:80]!r}")
    for job_id in dlq_by_job.keys() - accepted.keys():
        i3.fail(f"DLQ entry for {job_id}, which was never accepted")
    i3.detail = {
        "dlq_entries": len(dlq),
        "reasons": dict(Counter(f["dlq_reason"] for _i, f in dlq)),
    }

    # ---- I5: drained. Nothing left in the stream, the PEL, or the delayed set.
    stream_len = int(await redis.xlen(keys.stream))
    summary: Any = await redis.xpending(keys.stream, group)
    pel = int(summary["pending"])
    delayed = int(await redis.zcard(keys.delayed))
    i5 = Check(
        stream_len == pel == delayed == 0, {"stream": stream_len, "pel": pel, "delayed": delayed}
    )

    invariants: dict[str, dict[str, Any]] = {
        "I1_no_loss": i1.as_dict(),
        "I2_no_duplicate_effects": i2.as_dict(),
        "I2b_no_duplicate_results": i2b.as_dict(),
        "I3_dlq_correct": i3.as_dict(),
    }

    # ---- I4: the faults actually happened. If nothing was ever redelivered, the run
    # proved nothing, so it fails.
    if evidence is not None:
        m = minimums or Minimums()
        seen = {
            **evidence,
            **{k: counters[k] for k in ("reclaimed", "duplicates_suppressed", "timeouts")},
        }
        i4 = Check(True, {"observed": seen, "minimums": vars(m)})
        for name, minimum in vars(m).items():
            if seen.get(name, 0) < minimum:
                i4.fail(f"{name} = {seen.get(name, 0)} < {minimum}")
        invariants["I4_faults_happened"] = i4.as_dict()
    invariants["I5_drained"] = i5.as_dict()

    raw_histogram: Any = await redis.hgetall(keys.reclaims)
    histogram = {int(k): int(v) for k, v in raw_histogram.items()}
    # A crashy job's single entry is reclaimed exactly once at each delivery count from
    # 2 to max_deliveries + 1 (each claim raises the count by one, and each run crashes).
    # Subtracting that leaves how far every OTHER job's delivery count climbed: the
    # margin under max_deliveries that ADR-008's sizing is about.
    crashy = sum(1 for kind in accepted.values() if kind == "crashy")
    others = {n: c - (crashy if 2 <= n <= max_deliveries + 1 else 0) for n, c in histogram.items()}
    others = {n: c for n, c in others.items() if c}
    return {
        "passed": all(v["ok"] for v in invariants.values()),
        "invariants": invariants,
        "counters": counters,
        # How many reclaims found an entry at each delivery count: how close anything
        # came to max_deliveries (ADR-008). Crashy jobs reach max_deliveries + 1.
        "reclaims_by_delivery": dict(sorted(histogram.items())),
        "reclaims_by_delivery_excluding_crashy": dict(sorted(others.items())),
        "max_delivery_excluding_crashy": max(others, default=1),
    }
