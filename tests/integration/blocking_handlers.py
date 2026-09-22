"""Test-only handlers, loaded by real worker subprocesses via
`--handlers tests.integration.blocking_handlers:registry`.

`hog_on_loop` is the anti-pattern ADR-028 exists to prevent: CPU-bound work in an async
handler, blocking the event loop. It's here as a control, to prove the lease measurement
in test_blocking_handlers.py really can detect starved heartbeats.
"""

import hashlib
import time
from typing import Any

from ftq.models import Job
from ftq.registry import JobContext, Registry

registry = Registry()


@registry.register("hog_on_loop")
async def hog_on_loop(ctx: JobContext) -> dict[str, Any]:
    deadline = time.monotonic() + float(ctx.job.payload["seconds"])
    digest = b"x"
    while time.monotonic() < deadline:  # never awaits: the loop is stuck here
        digest = hashlib.sha256(digest).digest()
    return {"digest": digest.hex()}


def blocking_io(job: Job) -> dict[str, Any]:
    """A synchronous SDK call, say: blocks its thread, releases the GIL while waiting."""
    time.sleep(float(job.payload["seconds"]))
    return {"slept": job.payload["seconds"]}


registry.register_sync("blocking_io", pool="thread")(blocking_io)
