"""Producer API: `enqueue()` puts a job on the queue.

Batched enqueue and backpressure (`QueueFull`) arrive in Phase 3.
"""

from typing import Any

import redis.asyncio as aioredis

from ftq.config import Settings
from ftq.keys import Keys
from ftq.lua import register
from ftq.models import Job, new_job_id


class Client:
    def __init__(self, redis: aioredis.Redis, settings: Settings) -> None:
        self._settings = settings
        self._keys = Keys(settings.queue)
        self._enqueue = register(redis, "enqueue")

    async def enqueue(
        self,
        job_type: str,
        payload: dict[str, Any] | None = None,
        *,
        idempotency_key: str | None = None,
    ) -> str:
        """Add a job and return its job_id.

        Once this returns, the job is accepted: it will end in exactly one terminal
        state (SPEC §4). With `idempotency_key`, a repeat enqueue within
        `idempotency_ttl_seconds` adds nothing and returns the ORIGINAL job_id, even if
        the repeat's payload differs (the key is the producer's promise that it's the
        same job; we don't compare payloads).
        """
        job = Job(
            job_id=new_job_id(),
            type=job_type,
            payload=payload or {},
            idempotency_key=idempotency_key or "",
        )
        keys = [self._keys.stream]
        if idempotency_key:
            keys.append(self._keys.idem(idempotency_key))
        args: list[str] = [job.job_id, str(self._settings.idempotency_ttl_seconds)]
        for name, value in job.to_fields().items():
            args += [name, value]
        job_id, _created = await self._enqueue(keys=keys, args=args)
        return str(job_id)
