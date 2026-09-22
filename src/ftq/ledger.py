"""EffectLedger: the stand-in for a downstream system that honors idempotency keys.

Handlers never "send the email" directly; they call `ledger.apply(key, ...)`, which
performs the effect only the first time it sees `key`. This is how real systems get
effectively-once side effects from at-least-once delivery: Stripe, SES, and most payment
APIs accept an idempotency key. It only works if the downstream system honors the key.
A plain SMTP server wouldn't, and no queue design can fix that (SPEC §4).

The key must be stable across redeliveries of the same job, so handlers derive it from
`job_id` (ADR-019).
"""

import redis.asyncio as aioredis

from ftq.keys import Keys
from ftq.lua import register


class EffectLedger:
    def __init__(self, redis: aioredis.Redis, keys: Keys, ttl_seconds: int) -> None:
        self._keys = keys
        self._ttl = ttl_seconds
        self._script = register(redis, "ledger")

    async def apply(self, effect_key: str, **fields: str) -> bool:
        """Perform the effect named `effect_key` unless it was already performed.

        Returns True if applied now, False if suppressed as a repeat. Safe to re-send
        after a lost reply: a repeat changes nothing but a counter.
        """
        args: list[str] = [effect_key, str(self._ttl)]
        for name, value in fields.items():
            args += [name, value]
        applied = await self._script(
            keys=[self._keys.ledger(effect_key), self._keys.effects, self._keys.stats],
            args=args,
        )
        return int(applied) == 1
