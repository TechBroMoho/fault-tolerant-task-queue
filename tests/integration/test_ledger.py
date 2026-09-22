"""EffectLedger: an effect key is applied at most once, and the append-only effects log
records exactly the applied effects (the evidence the chaos verifier counts)."""

import pytest
import redis.asyncio as aioredis

from ftq.keys import Keys
from ftq.ledger import EffectLedger
from ftq.metrics import read_counters

from .helpers import entries

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


async def test_effect_applied_once(r: aioredis.Redis, keys: Keys) -> None:
    ledger = EffectLedger(r, keys, ttl_seconds=0)
    assert await ledger.apply("email:1", to="ada@example.com") is True
    assert await ledger.apply("email:1", to="ada@example.com") is False
    assert await ledger.apply("email:2", to="bob@example.com") is True

    log = await entries(r, keys.effects)
    assert [fields for _id, fields in log] == [
        {"key": "email:1", "to": "ada@example.com"},
        {"key": "email:2", "to": "bob@example.com"},
    ]
    counters = await read_counters(r, keys)
    assert (counters["effects_applied"], counters["effects_suppressed"]) == (2, 1)


async def test_marker_ttl(r: aioredis.Redis, keys: Keys) -> None:
    await EffectLedger(r, keys, ttl_seconds=0).apply("forever")
    assert await r.ttl(keys.ledger("forever")) == -1
    await EffectLedger(r, keys, ttl_seconds=600).apply("bounded")
    assert 590 < await r.ttl(keys.ledger("bounded")) <= 600
