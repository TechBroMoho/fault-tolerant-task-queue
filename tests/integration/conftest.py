"""Fixtures for integration tests against a real Redis (SPEC §3.7: never mock Redis).

If Redis is unreachable the tests FAIL with a hint rather than skip: a skipped
integration suite would let `make check` go green without testing anything
(DECISIONS.md ADR-012).
"""

import os
from collections.abc import AsyncIterator

import pytest
import pytest_asyncio
import redis.asyncio as aioredis
from redis.exceptions import ConnectionError as RedisConnectionError

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
