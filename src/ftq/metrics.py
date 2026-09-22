"""Queue counters, kept in one Redis hash per queue (`Keys.stats`).

The counters are incremented *inside* the Lua scripts that perform the transition, so a
counter can never disagree with what actually happened (no "did the work, crashed
before counting" gap). Phase 1 counters:

- processed: first successful commits (commit.lua)
- duplicates_suppressed: commits that found the job already SUCCEEDED (commit.lua).
  This includes a commit re-sent after a lost reply, which is also a suppressed duplicate.
- effects_applied / effects_suppressed: ledger calls that did / didn't apply (ledger.lua)
"""

from typing import Any

import redis.asyncio as aioredis

from ftq.keys import Keys

COUNTERS = ("processed", "duplicates_suppressed", "effects_applied", "effects_suppressed")


async def read_counters(redis: aioredis.Redis, keys: Keys) -> dict[str, int]:
    """All known counters, with 0 for any that were never incremented."""
    # redis-py types replies as bytes | str; with decode_responses=True they are str.
    raw: dict[Any, Any] = await redis.hgetall(keys.stats)
    return {name: int(raw.get(name, 0)) for name in COUNTERS}
