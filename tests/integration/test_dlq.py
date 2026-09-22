"""The DLQ CLI and requeue, end to end (SPEC §4: `dlq list`, `dlq requeue <id|--all>`)."""

import asyncio
import json
import os
import sys

import pytest
import redis.asyncio as aioredis

from ftq.client import Client
from ftq.config import Settings
from ftq.keys import Keys
from ftq.metrics import read_counters
from ftq.registry import JobContext, Registry

from .helpers import REPO_ROOT, entries, fast, hash_of, running_worker, wait_for

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


async def _ftq(settings: Settings, *args: str) -> tuple[int, str, str]:
    """Run the real CLI (`python -m ftq ...`) against the test queue."""
    proc = await asyncio.create_subprocess_exec(
        sys.executable,
        "-m",
        "ftq",
        *args,
        env={**os.environ, "FTQ_REDIS_URL": settings.redis_url, "FTQ_QUEUE": settings.queue},
        cwd=REPO_ROOT,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    out, err = await asyncio.wait_for(proc.communicate(), timeout=15)
    assert proc.returncode is not None
    return proc.returncode, out.decode(), err.decode()


async def test_dlq_list_and_requeue_via_cli_without_repeating_effects(
    r: aioredis.Redis, settings: Settings, keys: Keys
) -> None:
    """A job applies its effect, then fails until it's DEAD. Once the bug is "fixed", the
    operator requeues it, and it succeeds. Its effect is NOT applied a second time,
    because it keeps its job_id and so its ledger key."""
    s = fast(settings, max_attempts=2)
    fixed = False
    registry = Registry()

    @registry.register("charge")
    async def charge(ctx: JobContext) -> str:
        await ctx.ledger.apply(f"charge:{ctx.job.job_id}")  # the effect happens first...
        if not fixed:
            raise RuntimeError("bug after the charge")  # ...then the handler fails
        return "ok"

    job_id = await Client(r, s).enqueue("charge")
    async with running_worker(r, s, registry):

        async def dead() -> bool:
            return (await hash_of(r, keys.done(job_id))).get("state") == "DEAD"

        await wait_for(dead)

        code, out, err = await _ftq(s, "dlq", "list")
        assert code == 0, err
        [listed] = [json.loads(line) for line in out.splitlines()]
        assert (listed["job_id"], listed["type"], listed["reason"]) == (
            job_id,
            "charge",
            "max_attempts",
        )
        assert listed["attempts"] == 2 and "bug after the charge" in listed["error"]

        fixed = True
        code, out, err = await _ftq(s, "dlq", "requeue", job_id)
        assert (code, out.strip()) == (0, "requeued 1"), err

        async def succeeded() -> bool:
            return (await hash_of(r, keys.done(job_id))).get("state") == "SUCCEEDED"

        await wait_for(succeeded)

    code, out, _err = await _ftq(s, "dlq", "list")
    assert (code, out) == (0, "")
    counters = await read_counters(r, keys)
    assert counters["requeued"] == 1
    assert counters["effects_applied"] == 1  # charged once, across 3 runs
    assert counters["effects_suppressed"] == 2
    assert [f["key"] for _id, f in await entries(r, keys.effects)] == [f"charge:{job_id}"]
    assert await r.xlen(keys.results) == 1


async def test_dlq_requeue_cli_argument_handling(
    r: aioredis.Redis, settings: Settings, keys: Keys
) -> None:
    code, _out, err = await _ftq(settings, "dlq", "requeue")
    assert code != 0 and "--all" in err  # neither ids nor --all
    code, out, err = await _ftq(settings, "dlq", "requeue", "no-such-job")
    assert (code, out.strip()) == (0, "requeued 0")
    assert "not DEAD" in err
    code, out, _err = await _ftq(settings, "dlq", "requeue", "--all")
    assert (code, out.strip()) == (0, "requeued 0")
