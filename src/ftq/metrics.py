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
- timeouts: handler runs that exceeded their timeout and were then retried or
  dead-lettered by their owner (retry.lua, dead.lua; ADR-030)
- rejected / blocked: enqueues refused because the queue was full, in reject mode /
  enqueues that had to wait for room in block mode, counted once per job however
  long it waited (enqueue.lua; ADR-031)

`snapshot()` adds the queue's current shape (depth, in flight, delayed, DLQ size,
consumers) for `ftq stats`.
"""

from typing import Any

import redis.asyncio as aioredis
from redis.exceptions import ResponseError

from ftq.config import Settings
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
    "timeouts",
    "rejected",
    "blocked",
)


async def read_counters(redis: aioredis.Redis, keys: Keys) -> dict[str, int]:
    """All known counters, with 0 for any that were never incremented."""
    # redis-py types replies as bytes | str; with decode_responses=True they are str.
    raw: dict[Any, Any] = await redis.hgetall(keys.stats)
    return {name: int(raw.get(name, 0)) for name in COUNTERS}


async def snapshot(redis: aioredis.Redis, settings: Settings) -> dict[str, Any]:
    """The queue right now, plus every counter. Read in one pipelined round trip (not a
    transaction), so the numbers are a near-simultaneous view, not an atomic one."""
    keys = Keys(settings.queue)
    pipe = redis.pipeline(transaction=False)
    pipe.xlen(keys.stream)
    pipe.zcard(keys.delayed)
    pipe.xlen(keys.dead)
    pipe.exists(keys.full)
    pipe.hgetall(keys.stats)
    stream_len, delayed, dead, full_flag, raw = await pipe.execute()
    try:
        summary: Any = await redis.xpending(keys.stream, settings.group)
        in_flight = int(summary["pending"])
        group_info: Any = await redis.xinfo_consumers(keys.stream, settings.group)
        consumers = len(group_info)
    except ResponseError:  # no stream or group yet: no worker has ever started
        in_flight, consumers = 0, 0
    depth = int(stream_len) + int(delayed)
    return {
        "queue": settings.queue,
        # What backpressure compares with the watermarks (ADR-031).
        "depth": depth,
        # Of the stream's entries, those delivered and not yet acked (the PEL).
        "in_flight": in_flight,
        "undelivered": int(stream_len) - in_flight,
        "delayed": int(delayed),
        "dlq": int(dead),
        "consumers": consumers,
        # The flag only changes on an enqueue, so report it the way the next enqueue
        # would see it: still full only if depth hasn't fallen below the low watermark.
        "full": bool(full_flag) and depth >= settings.low_watermark,
        "watermarks": {"high": settings.high_watermark, "low": settings.low_watermark},
        "counters": {name: int(raw.get(name, 0)) for name in COUNTERS},
    }
