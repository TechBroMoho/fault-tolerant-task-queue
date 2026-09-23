"""Mutation checks for the chaos verifier (SPEC §7): plant a bug, run a scenario that
produces duplicate deliveries, and check the verifier catches it.

The scenario runs two in-process workers against real Redis with a 0.5 s lease. Its slow
jobs don't heartbeat and run past the lease, so a reaper always reclaims them while the
first holder is still running: both holders run the handler, apply the effect, and
commit. With the real scripts the second copy is suppressed; with a planted bug it isn't,
and the append-only logs show it.

A bug is planted by rewriting one Lua script's source before the workers load it
(`ftq.lua.script_source`), and the rewrite must match exactly once, so a mutant can
never silently be the original script.
"""

import asyncio
from collections.abc import Callable
from typing import Any

import pytest
import redis.asyncio as aioredis

import ftq.lua
from chaos.verifier import verify
from ftq.client import Client, NewJob
from ftq.config import Settings
from ftq.handlers import registry as built_in
from ftq.handlers.faults import InjectedFailure
from ftq.keys import Keys
from ftq.metrics import read_counters
from ftq.registry import JobContext, Registry

from .helpers import fast, hash_of, pel_size, running_worker, wait_for, with_

pytestmark = [pytest.mark.integration, pytest.mark.asyncio, pytest.mark.slow]

MAX_ATTEMPTS = 3


def plant(monkeypatch: pytest.MonkeyPatch, script: str, old: str, new: str) -> None:
    original: Callable[[str], str] = ftq.lua.script_source
    assert original(script).count(old) == 1, f"mutation target not found once in {script}.lua"

    def mutated(name: str) -> str:
        source = original(name)
        return source.replace(old, new) if name == script else source

    monkeypatch.setattr(ftq.lua, "script_source", mutated)


async def run_scenario(
    r: aioredis.Redis, s: Settings, keys: Keys, registry: Registry, jobs: list[tuple[str, NewJob]]
) -> dict[str, str]:
    """Enqueue `jobs` (kind, job), run workers A and B until every job is terminal and
    nothing is pending, then stop them (their drain waits for any redundant copy still
    running). Returns job_id -> kind for the verifier."""
    ids = await Client(r, s).enqueue_many([job for _kind, job in jobs])
    accepted = {job_id: kind for job_id, (kind, _job) in zip(ids, jobs, strict=True)}

    async def settled() -> bool:
        states = [(await hash_of(r, keys.done(j))).get("state") for j in ids]
        return (
            all(states)
            and await pel_size(r, keys.stream, s.group) == 0
            and await r.zcard(keys.delayed) == 0
        )

    async with running_worker(r, s, registry, "A"), running_worker(r, s, registry, "B"):
        await wait_for(settled, within=20)
    return accepted


def duplicate_scenario() -> list[tuple[str, NewJob]]:
    return (
        [("normal", NewJob("send_email")) for _ in range(20)]
        + [("slow", NewJob("slow", {"seconds": 1.0})) for _ in range(3)]
        + [("poison", NewJob("poison"))]
    )


async def check(
    r: aioredis.Redis, s: Settings, keys: Keys, accepted: dict[str, str]
) -> dict[str, Any]:
    report = await verify(r, keys, s.group, accepted, MAX_ATTEMPTS, s.max_deliveries)
    return {name: inv["ok"] for name, inv in report["invariants"].items()} | {"_report": report}


async def test_control_the_real_scripts_pass_and_the_scenario_has_duplicates(
    r: aioredis.Redis, settings: Settings, keys: Keys
) -> None:
    s = fast(settings, max_attempts=MAX_ATTEMPTS)
    accepted = await run_scenario(r, s, keys, built_in, duplicate_scenario())
    result = await check(r, s, keys, accepted)
    assert result["_report"]["passed"], result["_report"]["invariants"]
    counters = await read_counters(r, keys)
    # Every slow job ran twice and committed twice: the mutations below have something
    # to get wrong. (Suppressed at commit and at the ledger.)
    assert counters["reclaimed"] >= 3
    assert counters["duplicates_suppressed"] >= 3
    assert counters["effects_suppressed"] >= 3


async def test_ledger_without_nx_fails_i2(
    r: aioredis.Redis, settings: Settings, keys: Keys, monkeypatch: pytest.MonkeyPatch
) -> None:
    """ledger.lua applies every call, not just the first: duplicate effects."""
    plant(
        monkeypatch,
        "ledger",
        "redis.call('SET', KEYS[1], '1', 'NX')",
        "redis.call('SET', KEYS[1], '1')",
    )
    s = fast(settings, max_attempts=MAX_ATTEMPTS)
    accepted = await run_scenario(r, s, keys, built_in, duplicate_scenario())
    result = await check(r, s, keys, accepted)
    assert result["I2_no_duplicate_effects"] is False
    violations = result["_report"]["invariants"]["I2_no_duplicate_effects"]["violations"]
    assert any(v.startswith("slow:") and v.endswith(": 2 effects") for v in violations)
    assert result["I2b_no_duplicate_results"] is True  # commit.lua still dedups results


async def test_commit_without_the_done_check_fails_i2b(
    r: aioredis.Redis, settings: Settings, keys: Keys, monkeypatch: pytest.MonkeyPatch
) -> None:
    """commit.lua records every commit as a first success: duplicate results."""
    plant(monkeypatch, "commit", "if state == 'SUCCEEDED' then", "if false then")
    s = fast(settings, max_attempts=MAX_ATTEMPTS)
    accepted = await run_scenario(r, s, keys, built_in, duplicate_scenario())
    result = await check(r, s, keys, accepted)
    assert result["I2b_no_duplicate_results"] is False
    violations = result["_report"]["invariants"]["I2b_no_duplicate_results"]["violations"]
    assert any("(slow, SUCCEEDED): 2 results" in v for v in violations)
    assert result["I2_no_duplicate_effects"] is True  # the ledger still dedups effects


# ---------------------------------------------------------------- retry ownership

STALL_S = 1.0  # > the 0.5 s lease: the first holder is always reclaimed mid-run


async def stale_then_fail(ctx: JobContext) -> dict[str, Any]:
    """Worker A's run stalls past its lease and then fails; worker B's run of the same
    job succeeds. B heartbeats and A doesn't, and A has one slot (so its own reaper
    never runs while it's busy). So B reclaims the entry and still owns it, mid-run,
    when A's retry arrives: the exact window only the ownership check guards."""
    await asyncio.sleep(STALL_S)
    if ctx.worker_id == "A":
        raise InjectedFailure("A's stale run failed after its lease was lost")
    applied = await ctx.ledger.apply(f"slow:{ctx.job.job_id}")
    return {"applied_now": applied}


async def run_stale_retry_window(r: aioredis.Redis, s: Settings, keys: Keys) -> dict[str, str]:
    stalls, heartbeats = Registry(), Registry()
    stalls.register("slow", heartbeat=False)(stale_then_fail)
    heartbeats.register("slow")(stale_then_fail)
    [job_id] = await Client(r, s).enqueue_many([NewJob("slow")])
    async with running_worker(r, with_(s, concurrency=1), stalls, "A"):
        # A takes the job first; only then does B start, so B can only get it by reclaim.
        async def a_has_it() -> bool:
            reply: Any = await r.xpending_range(keys.stream, s.group, "-", "+", 1)
            return bool(reply) and reply[0]["consumer"] == "A"

        await wait_for(a_has_it)
        async with running_worker(r, s, heartbeats, "B"):

            async def settled() -> bool:
                done = (await hash_of(r, keys.done(job_id))).get("state") == "SUCCEEDED"
                return (
                    done
                    and await pel_size(r, keys.stream, s.group) == 0
                    and (await r.zcard(keys.delayed) == 0)
                )

            await wait_for(settled, within=20)
    return {job_id: "slow"}


async def test_control_stale_retry_is_refused(
    r: aioredis.Redis, settings: Settings, keys: Keys
) -> None:
    s = fast(settings, max_attempts=MAX_ATTEMPTS)
    accepted = await run_stale_retry_window(r, s, keys)
    counters = await read_counters(r, keys)
    assert counters["reclaimed"] == 1  # B took it over from A
    assert (counters["retried"], counters["lease_lost"]) == (0, 1)  # A's retry: LEASE_LOST
    assert (await check(r, s, keys, accepted))["_report"]["passed"]


async def test_retry_without_the_ownership_check_is_invisible_to_the_verifier(
    r: aioredis.Redis, settings: Settings, keys: Keys, monkeypatch: pytest.MonkeyPatch
) -> None:
    """SPEC §7 asks that bypassing retry.lua's ownership check make I1 (or I2/I2b) fail.
    It can't, and this test pins down why (ADR-037). The mutant IS live: A's stale retry
    is scheduled while B owns the entry (retried = 1, nothing refused). But every later
    guard absorbs it. B's commit is first-wins, so the job is SUCCEEDED once. The
    needless retry's run finds it SUCCEEDED (a suppressed duplicate, or TERMINAL in
    retry.lua), and its effect is suppressed by the ledger. A stale retry that arrives
    after B's own transition instead finds the entry deleted (ENTRY_MISSING) or the job
    terminal. The only trace is a wasted run. The ownership check's own promise ("a
    stale worker changes nothing") is enforced by the Phase 2 script and stale-worker
    tests, which do fail under this mutation (PROGRESS.md, Phase 4)."""
    plant(monkeypatch, "retry", "if #p == 0 or p[1][2] ~= ARGV[3] then", "if false then")
    s = fast(settings, max_attempts=MAX_ATTEMPTS)
    accepted = await run_stale_retry_window(r, s, keys)
    counters = await read_counters(r, keys)
    assert counters["reclaimed"] == 1
    # The mutant acted: A's stale retry was scheduled and deleted B's entry. (B's next
    # heartbeat then finds the entry gone: that is the one lease_lost.)
    assert (counters["retried"], counters["lease_lost"]) == (1, 1)
    result = await check(r, s, keys, accepted)
    assert result["_report"]["passed"], result["_report"]["invariants"]
