"""The delayed-retry scheduler: move retries whose backoff has elapsed back to the stream.

A failed job waits in the `delayed` sorted set, scored by its due time on Redis's clock
(retry.lua). Every worker runs this mover. That's safe because schedule.lua removes a due
job from the set and adds it to the stream in one atomic step, so no retry is moved
twice or dropped (ADR-026).
"""

from typing import Any

import redis.asyncio as aioredis

from ftq.config import Settings
from ftq.keys import Keys
from ftq.lua import register


class Scheduler:
    def __init__(self, redis: aioredis.Redis, settings: Settings) -> None:
        self._settings = settings
        self._keys = Keys(settings.queue)
        self._schedule = register(redis, "schedule")

    async def move_due(self) -> int:
        """Move up to `scheduler_batch` due retries into the stream; return how many."""
        moved: Any = await self._schedule(
            keys=[self._keys.delayed, self._keys.stream, self._keys.stats],
            args=[str(self._settings.scheduler_batch)],
        )
        return int(moved)
