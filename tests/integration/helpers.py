"""Shared helpers for integration tests (kept out of conftest so tests can import them)."""

import asyncio
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from typing import Any

import pytest
import redis.asyncio as aioredis

from ftq.config import Settings
from ftq.registry import Registry
from ftq.worker import Worker


@asynccontextmanager
async def running_worker(
    redis: aioredis.Redis, settings: Settings, registry: Registry
) -> AsyncIterator[Worker]:
    """Run a Worker in the background for the duration of the block, then stop it."""
    worker = Worker(redis, settings, registry, worker_id="test-worker")
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


async def add_entry(r: aioredis.Redis, stream: str, fields: dict[str, str]) -> None:
    """XADD `fields` verbatim (e.g. to put a copy of an existing job back on the stream)."""
    loose: dict[Any, Any] = dict(fields)  # xadd's declared param type is invariant
    await r.xadd(stream, loose)
