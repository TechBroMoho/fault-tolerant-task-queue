"""The reaper: reclaim entries whose lease expired, and prune dead consumer records.

A lease is an entry's idle time in the Pending Entries List. A healthy worker resets it
with heartbeats (ADR-025). When the idle time passes `visibility_timeout`, the owner has
crashed, stalled, or lost its network, and any worker may take the entry over with
XAUTOCLAIM (ADR-023). Every worker runs the reaper; claims are atomic in Redis, so two
reapers can never both claim one entry.

The worker calls `reclaim()` from its fetch loop with its number of free slots, so
reclaimed jobs count against the same in-flight cap as fresh ones.
"""

import logging
from dataclasses import dataclass
from typing import Any

import redis.asyncio as aioredis

from ftq.config import Settings
from ftq.keys import Keys
from ftq.lua import register

log = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class Claimed:
    entry_id: str
    fields: dict[str, str]
    deliveries: int  # the entry's delivery count, including this claim

    def is_suspect(self, settings: Settings) -> bool:
        """Redelivered often enough to suspect it crashes workers, but not so often that
        it goes to the DLQ unrun (ADR-035). A worker runs one suspect at a time."""
        return settings.suspect_deliveries <= self.deliveries <= settings.max_deliveries


def _pairs(flat: list[str]) -> dict[str, str]:
    """A Lua-returned {name, value, name, value, ...} list as a dict."""
    return dict(zip(flat[::2], flat[1::2], strict=True))


class Reaper:
    def __init__(self, redis: aioredis.Redis, settings: Settings, worker_id: str) -> None:
        self._settings = settings
        self._keys = Keys(settings.queue)
        self._worker_id = worker_id
        self._reclaim = register(redis, "reclaim")
        self._prune = register(redis, "prune_consumers")
        # XAUTOCLAIM scans the PEL in id order from a cursor. Keeping the cursor between
        # calls means a long PEL is scanned a slice at a time instead of from the start
        # on every pass. "0-0" = start over.
        self._cursor = "0-0"

    async def reclaim(self, count: int, suspect_slots: int = 1) -> tuple[list[Claimed], bool]:
        """Claim up to `count` expired entries, of which at most `suspect_slots` suspects
        (ADR-035). Returns (claimed, more_to_scan)."""
        reply: Any = await self._reclaim(
            keys=[self._keys.stream, self._keys.stats, self._keys.reclaims],
            args=[
                self._settings.group,
                self._worker_id,
                str(int(self._settings.visibility_timeout * 1000)),
                self._cursor,
                str(count),
                str(suspect_slots),
                str(self._settings.suspect_deliveries),
                str(self._settings.max_deliveries),
            ],
        )
        cursor, claimed, deleted = reply
        self._cursor = str(cursor)
        if deleted:
            # Pending ids whose entry is gone; XAUTOCLAIM already dropped them from the
            # PEL. Our exits always ack before deleting, so this means something outside
            # ftq deleted stream entries. Loud, because each one is a job we can't run.
            log.error(
                "worker %s: reaper found %d pending entries with no stream data: %s",
                self._worker_id,
                len(deleted),
                deleted,
            )
        return (
            [Claimed(str(eid), _pairs(flat), int(n)) for eid, flat, n in claimed],
            self._cursor != "0-0",
        )

    async def prune_consumers(self) -> list[str]:
        """Delete consumers idle past `consumer_prune_idle` that own NO pending entries.

        `XGROUP DELCONSUMER` discards a consumer's pending entries, so deleting one that
        still owns work would silently lose those jobs. prune_consumers.lua checks the
        PEL and deletes in one atomic step (ADR-029).
        """
        reply: Any = await self._prune(
            keys=[self._keys.stream, self._keys.stats],
            args=[
                self._settings.group,
                str(int(self._settings.consumer_prune_idle * 1000)),
                self._worker_id,
            ],
        )
        return [str(name) for name in reply]
