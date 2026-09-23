"""Queue counters, kept in one Redis hash per queue (`Keys.stats`).

The counters are incremented *inside* the Lua scripts that perform the transition, so a
counter can never disagree with what actually happened (no "did the work, crashed
before counting" gap). Counters count events, not states: a job that went DEAD and later
succeeded late counts once in `dead` and once in `late_successes`.

- processed: first successful commits (commit.lua)
- duplicates_suppressed: transitions that found the job already terminal, so this copy's
  work was redundant: a commit finding SUCCEEDED (including a commit re-sent after a lost
  reply), or a retry/DLQ move by the entry's owner finding SUCCEEDED or DEAD
- effects_applied / effects_suppressed: ledger calls that did / didn't apply (ledger.lua)
- retried: failures scheduled for a retry (retry.lua)
- scheduled: due retries moved back into the stream (schedule.lua)
- dead: jobs moved to the DLQ, by any path (dead.lua)
- late_successes: commits that replaced DEAD with SUCCEEDED (commit.lua, ADR-009)
- requeued: DLQ jobs put back on the stream (requeue.lua)
- reclaimed: entries a reaper took over after their lease expired (reclaim.lua)
- heartbeats: successful lease extensions (heartbeat.lua)
- lease_lost: heartbeat/retry/DLQ calls refused because the caller no longer owned the
  entry (heartbeat.lua, retry.lua, dead.lua). An upper bound on real lease losses: a
  retry or DLQ move re-sent after a lost reply is also refused (its first run already
  acked the entry) and counts here too.
- consumers_pruned: idle consumer records with no pending entries deleted
  (prune_consumers.lua)
"""

from typing import Any

import redis.asyncio as aioredis

from ftq.keys import Keys

COUNTERS = (
    "processed",
    "duplicates_suppressed",
    "effects_applied",
    "effects_suppressed",
    "retried",
    "scheduled",
    "dead",
    "late_successes",
    "requeued",
    "reclaimed",
    "heartbeats",
    "lease_lost",
    "consumers_pruned",
)


async def read_counters(redis: aioredis.Redis, keys: Keys) -> dict[str, int]:
    """All known counters, with 0 for any that were never incremented."""
    # redis-py types replies as bytes | str; with decode_responses=True they are str.
    raw: dict[Any, Any] = await redis.hgetall(keys.stats)
    return {name: int(raw.get(name, 0)) for name in COUNTERS}
