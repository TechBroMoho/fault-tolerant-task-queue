"""Shared helpers for integration tests (kept out of conftest so tests can import them)."""

import asyncio
import json
import os
import sys
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import pytest
import redis.asyncio as aioredis

from ftq.config import Settings
from ftq.registry import Registry
from ftq.worker import Worker

REPO_ROOT = Path(__file__).resolve().parents[2]

# Phase 2 timings for tests: a 0.5 s lease, so reclaims happen fast, and heartbeats five
# times per lease. Retry backoff is tiny so flaky jobs finish quickly.
FAST: dict[str, Any] = {
    "visibility_timeout": 0.5,
    "heartbeat_interval": 0.1,
    "reap_interval": 0.05,
    "scheduler_interval": 0.02,
    "job_backoff_base": 0.01,
    "job_backoff_cap": 0.05,
}


def with_(settings: Settings, **overrides: Any) -> Settings:
    """A validated copy. (`model_copy(update=...)` skips validation, which could let a
    test run with a configuration the real worker would refuse.)"""
    return Settings.model_validate({**settings.model_dump(), **overrides})


def fast(settings: Settings, **overrides: Any) -> Settings:
    return with_(settings, **{**FAST, **overrides})


@asynccontextmanager
async def running_worker(
    redis: aioredis.Redis,
    settings: Settings,
    registry: Registry,
    worker_id: str = "test-worker",
) -> AsyncIterator[Worker]:
    """Run a Worker in the background for the duration of the block, then stop it."""
    worker = Worker(redis, settings, registry, worker_id=worker_id)
    # Create the group before yielding, so the block can inspect the PEL right away.
    # (run() does it again; it's idempotent.)
    await worker.ensure_group()
    task = asyncio.create_task(worker.run())
    try:
        yield worker
    finally:
        worker.request_stop()
        await asyncio.wait_for(task, timeout=settings.shutdown_grace + 5)


async def wait_for(
    condition: Callable[[], Awaitable[bool]], within: float = 5.0, interval: float = 0.02
) -> None:
    """Poll `condition` until true; fail the test on timeout (no bare sleeps)."""
    deadline = asyncio.get_running_loop().time() + within
    while not await condition():
        if asyncio.get_running_loop().time() > deadline:
            pytest.fail(f"condition not met within {within}s")
        await asyncio.sleep(interval)


# redis-py declares its replies as broad unions (bytes | str | None ...). With
# decode_responses=True the real shapes are below; these helpers give tests precise types.


async def entries(r: aioredis.Redis, key: str) -> list[tuple[str, dict[str, str]]]:
    """All entries of a stream, oldest first."""
    reply: Any = await r.xrange(key)
    return list(reply)


async def hash_of(r: aioredis.Redis, key: str) -> dict[str, str]:
    reply: Any = await r.hgetall(key)
    return dict(reply)


async def pel_size(r: aioredis.Redis, stream: str, group: str) -> int:
    """Number of delivered-but-unacked entries in the group's Pending Entries List."""
    summary: Any = await r.xpending(stream, group)
    return int(summary["pending"])


@dataclass(frozen=True)
class Pending:
    entry_id: str
    owner: str
    idle_ms: int
    deliveries: int


async def pending(r: aioredis.Redis, stream: str, group: str) -> list[Pending]:
    """The PEL: every delivered-but-unacked entry with its owner, idle time, and count."""
    reply: Any = await r.xpending_range(stream, group, "-", "+", 1000)
    return [
        Pending(p["message_id"], p["consumer"], p["time_since_delivered"], p["times_delivered"])
        for p in reply
    ]


async def add_entry(r: aioredis.Redis, stream: str, fields: dict[str, str]) -> str:
    """XADD `fields` verbatim (e.g. to put a copy of an existing job back on the stream)."""
    loose: dict[Any, Any] = dict(fields)  # xadd's declared param type is invariant
    return str(await r.xadd(stream, loose))


async def deliver(r: aioredis.Redis, stream: str, group: str, consumer: str) -> str:
    """XREADGROUP one entry as `consumer` and never ack it: exactly what a worker that
    crashes right after fetching leaves behind. Returns the entry id."""
    reply: Any = await r.xreadgroup(group, consumer, {stream: ">"}, count=1)
    return str(reply[0][1][0][0])


async def consumers(r: aioredis.Redis, stream: str, group: str) -> dict[str, int]:
    """Consumer name -> its number of pending entries."""
    reply: Any = await r.xinfo_consumers(stream, group)
    return {c["name"]: int(c["pending"]) for c in reply}


@dataclass
class LeaseWatch:
    """Samples one entry's PEL record every few ms while it is in flight.

    Idle time is measured by Redis, so a lease that lapsed shows up as a large idle
    value whenever it is sampled. `samples` also shows whether this (test) event loop
    kept running: if the code under test blocked the loop, there would be almost none.
    """

    samples: int = 0
    max_idle_ms: int = 0
    owners: set[str] = field(default_factory=set)
    deliveries: set[int] = field(default_factory=set)


@asynccontextmanager
async def watching_lease(
    r: aioredis.Redis, stream: str, group: str, interval: float = 0.02
) -> AsyncIterator[LeaseWatch]:
    watch = LeaseWatch()

    async def sample() -> None:
        while True:
            for p in await pending(r, stream, group):
                watch.samples += 1
                watch.max_idle_ms = max(watch.max_idle_ms, p.idle_ms)
                watch.owners.add(p.owner)
                watch.deliveries.add(p.deliveries)
            await asyncio.sleep(interval)

    task = asyncio.create_task(sample())
    try:
        yield watch
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


# ---------------------------------------------------------------- real worker processes


async def start_worker_process(
    settings: Settings,
    handlers: str = "ftq.handlers:registry",
    new_session: bool = False,
    env: dict[str, str] | None = None,
) -> tuple[asyncio.subprocess.Process, str]:
    """Start `python -m ftq worker` and wait for its "started" log line.

    `env` adds FTQ_* variables (e.g. FTQ_VISIBILITY_TIMEOUT="0.5"). `new_session` gives
    the worker its own process group, so a test can signal the worker AND its pool
    children at once, like Ctrl-C in a terminal. Returns the process and its worker id
    (its consumer name), parsed from the log line.
    """
    full_env = {
        **os.environ,
        "FTQ_REDIS_URL": settings.redis_url,
        "FTQ_QUEUE": settings.queue,
        "FTQ_BLOCK_MS": "100",
        "FTQ_DONE_TTL_SECONDS": "0",
        "FTQ_LOG_LEVEL": "INFO",
        **(env or {}),
    }
    proc = await asyncio.create_subprocess_exec(
        sys.executable,
        "-m",
        "ftq",
        "worker",
        "--handlers",
        handlers,
        env=full_env,
        cwd=REPO_ROOT,  # so test-only handler modules (tests.integration.*) import
        stderr=asyncio.subprocess.PIPE,
        start_new_session=new_session,
    )
    line = await read_until(proc, ": started (")
    return proc, str(json.loads(line)["worker_id"])  # JSON logs by default (logs.py)


async def read_until(proc: asyncio.subprocess.Process, marker: str, within: float = 10) -> str:
    """Consume the worker's stderr until a line contains `marker`."""
    assert proc.stderr is not None
    seen: list[str] = []

    async def scan() -> str:
        assert proc.stderr is not None
        while line := (await proc.stderr.readline()).decode():
            seen.append(line)
            if marker in line:
                return line
        raise AssertionError(f"worker exited before logging {marker!r}:\n{''.join(seen)}")

    try:
        return await asyncio.wait_for(scan(), within)
    except TimeoutError:
        proc.kill()
        raise AssertionError(f"no {marker!r} within {within}s:\n{''.join(seen)}") from None
