"""A worker that starts while Redis is unreachable waits for it instead of crashing.

Found by the first chaos run: the supervisor restarted a crashed worker while that
worker's proxy was partitioned, and the new process died in its very first command
(creating the consumer group) with exit code 1. The fetch loop already waits out an
outage (ADR-022); startup must too, or an outage turns every restart into a crash loop.

The test points the worker at a port where nothing listens yet, then starts a plain TCP
forwarder to the real Redis on that port: Redis "comes back".
"""

import asyncio
import contextlib
import socket
from collections.abc import AsyncIterator

import pytest
import redis.asyncio as aioredis

from ftq.client import Client
from ftq.config import Settings, make_redis
from ftq.handlers import registry
from ftq.keys import Keys
from ftq.worker import Worker

from .conftest import REDIS_URL
from .helpers import hash_of, wait_for, with_

pytestmark = [pytest.mark.integration, pytest.mark.asyncio, pytest.mark.slow]


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


@contextlib.asynccontextmanager
async def forwarder(port: int, target_host: str, target_port: int) -> AsyncIterator[None]:
    """Listen on 127.0.0.1:`port` and pipe every connection to the target."""

    async def pipe(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        with contextlib.suppress(ConnectionError):
            while data := await reader.read(65536):
                writer.write(data)
                await writer.drain()
        writer.close()

    async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        up_reader, up_writer = await asyncio.open_connection(target_host, target_port)
        await asyncio.gather(pipe(reader, up_writer), pipe(up_reader, writer))

    server = await asyncio.start_server(handle, "127.0.0.1", port)
    try:
        yield
    finally:
        server.close()


async def test_worker_started_while_redis_is_unreachable_waits_then_runs(
    r: aioredis.Redis, settings: Settings, keys: Keys
) -> None:
    port = _free_port()
    target = aioredis.Redis.from_url(REDIS_URL).connection_pool.connection_kwargs
    # Same database as the test's own client (conftest: db 15), through the forwarder.
    db = target.get("db", 0)
    s = with_(settings, redis_url=f"redis://127.0.0.1:{port}/{db}", retry_attempts=0)
    job_id = await Client(r, settings).enqueue("send_email", {"to": "a@example.com"})

    worker_redis = make_redis(s)
    worker = Worker(worker_redis, s, registry, worker_id="late-starter")
    task = asyncio.create_task(worker.run())
    try:
        await asyncio.sleep(0.5)  # a few connection attempts fail meanwhile
        assert not task.done(), "the worker gave up while Redis was unreachable"
        async with forwarder(port, target["host"], target["port"]):

            async def done() -> bool:
                return (await hash_of(r, keys.done(job_id))).get("state") == "SUCCEEDED"

            await wait_for(done, within=10)
            worker.request_stop()
            await asyncio.wait_for(task, timeout=10)
    finally:
        if not task.done():
            task.cancel()
        await worker_redis.aclose()


async def test_worker_stopped_before_redis_was_ever_reachable_exits_cleanly(
    settings: Settings,
) -> None:
    s = with_(settings, redis_url=f"redis://127.0.0.1:{_free_port()}/0", retry_attempts=0)
    worker_redis = make_redis(s)
    worker = Worker(worker_redis, s, registry, worker_id="never-connected")
    task = asyncio.create_task(worker.run())
    await asyncio.sleep(0.3)
    worker.request_stop()
    await asyncio.wait_for(task, timeout=5)  # returns, no exception
    await worker_redis.aclose()
