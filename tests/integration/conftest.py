"""Fixtures for integration tests against a real Redis (SPEC §3.7: never mock Redis).

If Redis is unreachable the tests FAIL with a hint rather than skip: a skipped
integration suite would let `make check` go green without testing anything
(DECISIONS.md ADR-012).

Each test gets its own queue name, so its keys (`ftq:{<queue>}:*`) never collide with
another test's, and they're deleted afterwards.
"""

import os
import secrets
from collections.abc import AsyncIterator

import pytest
import pytest_asyncio
import redis.asyncio as aioredis
from redis.exceptions import ConnectionError as RedisConnectionError

from ftq.config import Settings, make_redis
from ftq.keys import Keys

REDIS_URL = os.environ.get("FTQ_REDIS_URL", "redis://localhost:6379/0")


@pytest_asyncio.fixture
async def redis_client() -> AsyncIterator[aioredis.Redis]:
    client = aioredis.Redis.from_url(REDIS_URL, socket_connect_timeout=2, socket_timeout=2)
    try:
        await client.ping()
    except RedisConnectionError as exc:
        await client.aclose()
        pytest.fail(f"Redis not reachable at {REDIS_URL} ({exc}). Run `make up` first.")
    try:
        yield client
    finally:
        await client.aclose()


@pytest.fixture
def settings() -> Settings:
    """Test config: unique queue, fast polling, no TTLs (the tests read every key)."""
    return Settings(
        redis_url=REDIS_URL,
        queue=f"test-{secrets.token_hex(4)}",
        block_ms=100,
        shutdown_grace=5.0,
        done_ttl_seconds=0,
    )


@pytest.fixture
def keys(settings: Settings) -> Keys:
    return Keys(settings.queue)


@pytest_asyncio.fixture
async def r(redis_client: aioredis.Redis, settings: Settings) -> AsyncIterator[aioredis.Redis]:
    """An ftq-configured client (decode_responses=True). Deletes the queue's keys after."""
    client = make_redis(settings)
    try:
        yield client
    finally:
        # SCAN glob: `{` and `}` are literal characters here (only * ? [ ] \ are special).
        doomed = [k async for k in client.scan_iter(match=f"{Keys(settings.queue).prefix}:*")]
        if doomed:
            await client.delete(*doomed)
        await client.aclose()
