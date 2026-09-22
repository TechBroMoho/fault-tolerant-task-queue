"""Load the Lua scripts in `ftq/scripts/`.

Why Lua at all: each state change (check `done`, record the result, append to the log,
ack, delete) must happen as one indivisible step. Redis runs a script to completion
with no other command interleaved, so a script is the simplest way to get a
check-then-act without races (ADR-017). MULTI/EXEC can't branch on a value it reads.
"""

from importlib.resources import files

import redis.asyncio as aioredis
from redis.commands.core import AsyncScript


def script_source(name: str) -> str:
    return (files("ftq") / "scripts" / f"{name}.lua").read_text(encoding="utf-8")


def register(redis: aioredis.Redis, name: str) -> AsyncScript:
    """Return a callable script. redis-py sends EVALSHA and falls back to loading the
    source on NOSCRIPT, so a Redis restart (empty script cache) is handled for us.
    """
    return redis.register_script(script_source(name))
