"""Phase 0 acceptance: the Compose Redis is up, is the pinned version, and is configured safely."""

import pytest
import redis.asyncio as aioredis

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


async def test_ping(redis_client: aioredis.Redis) -> None:
    assert await redis_client.ping() is True


async def test_pinned_redis_version(redis_client: aioredis.Redis) -> None:
    # The image tag is pinned in docker-compose.yml (DECISIONS.md ADR-004).
    info = await redis_client.info("server")
    assert info["redis_version"] == "8.8.3"


async def test_noeviction_policy(redis_client: aioredis.Redis) -> None:
    # An LRU/LFU policy would silently evict done/ledger keys and break idempotency
    # (DECISIONS.md ADR-013), so a misconfigured Redis must fail the suite.
    config = await redis_client.config_get("maxmemory-policy")
    assert config["maxmemory-policy"] == "noeviction"
