"""Test-only handlers, loaded by real worker subprocesses via
`--handlers tests.integration.blocking_handlers:registry`.

`hog_on_loop` is the anti-pattern ADR-028 exists to prevent: CPU-bound work in an async
handler, blocking the event loop. It's here as a control, to prove the lease measurement
in test_blocking_handlers.py really can detect starved heartbeats.
"""

import hashlib
import os
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


def crash_child_on_first_attempt(job: Job) -> dict[str, Any]:
    """Kills the pool child process (not the worker) on attempt 0, then succeeds."""
    if job.attempt == 0:
        os._exit(3)
    return {"attempt": job.attempt}


registry.register_sync("crash_child_on_first_attempt", pool="process")(crash_child_on_first_attempt)


def spin_after_marking(job: Job) -> dict[str, Any]:
    """Writes the file at payload["marker"] from INSIDE the pool child, then burns CPU for
    payload["seconds"]. The marker tells a test the child exists and is mid-job, so a
    signal sent after it really does hit a running child."""
    with open(job.payload["marker"], "w") as f:
        f.write(str(os.getpid()))
    deadline = time.monotonic() + float(job.payload["seconds"])
    digest = b"x"
    while time.monotonic() < deadline:
        digest = hashlib.sha256(digest).digest()
    return {"digest": digest.hex()}


registry.register_sync("spin_after_marking", pool="process")(spin_after_marking)


# ---------------------------------------------------------------- per-job timeouts (ADR-030)


def _record_run(job: Job) -> None:
    """Append this child's pid to payload["runs"]: one line per run of the job, so a test
    can see a run was restarted and which process ran it."""
    with open(job.payload["runs"], "a") as f:
        f.write(f"{os.getpid()}\n")


def hang_first_attempt_in_process(job: Job) -> dict[str, Any]:
    """Attempt 0 hangs (the timeout must kill its child); later attempts succeed."""
    _record_run(job)
    if job.attempt == 0:
        time.sleep(3600)
    return {"attempt": job.attempt, "pid": os.getpid()}


registry.register_sync("hang_first_attempt_in_process", pool="process", timeout=1.5)(
    hang_first_attempt_in_process
)


def spin_in_process(job: Job) -> dict[str, Any]:
    """Burns CPU for payload["seconds"]; an innocent bystander in a pool that gets reset."""
    _record_run(job)
    deadline = time.monotonic() + float(job.payload["seconds"])
    digest = b"x"
    while time.monotonic() < deadline:
        digest = hashlib.sha256(digest).digest()
    return {"attempt": job.attempt, "pid": os.getpid()}


registry.register_sync("spin_in_process", pool="process", timeout=30.0)(spin_in_process)
# The same bystander under a timeout it only meets if a pool reset restarts its clock:
# 2 s of work, 3 s timeout, with the reset landing ~1.5 s into its first run (ADR-039).
registry.register_sync("spin_in_process_3s_timeout", pool="process", timeout=3.0)(spin_in_process)


def hang_forever_in_thread(job: Job) -> None:
    """A blocking call that never returns, e.g. an SDK with no timeout of its own."""
    time.sleep(3600)


registry.register_sync("hang_forever_in_thread", pool="thread", timeout=0.5)(hang_forever_in_thread)


def spin_briefly_in_process(job: Job) -> dict[str, Any]:
    """~payload["seconds"] of CPU, under a timeout only a little longer than that."""
    return spin_in_process(job)


registry.register_sync("spin_briefly_in_process", pool="process", timeout=1.5)(
    spin_briefly_in_process
)
