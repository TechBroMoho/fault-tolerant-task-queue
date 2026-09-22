"""cpu_task: a CPU-bound job (iterated SHA-256), used to load workers in benchmarks."""

import hashlib
from typing import Any

from ftq.registry import JobContext


async def cpu_task(ctx: JobContext) -> dict[str, Any]:
    """Payload: optional `rounds` (int, default 1000).

    Deliberately runs on the event loop: it models real CPU-bound work, which holds the
    GIL whether or not it's on a thread. It has no side effects, so it needs no ledger.
    """
    rounds = int(ctx.job.payload.get("rounds", 1000))
    digest = ctx.job.job_id.encode()
    for _ in range(rounds):
        digest = hashlib.sha256(digest).digest()
    return {"rounds": rounds, "digest": digest.hex()}
