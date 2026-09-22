"""cpu_task: a CPU-bound job (iterated SHA-256), used to load workers in benchmarks."""

import hashlib
from typing import Any

from ftq.models import Job


def cpu_task(job: Job) -> dict[str, Any]:
    """Payload: optional `rounds` (int, default 1000).

    A plain function, registered to run in the worker's process pool (ADR-028). Hashing
    32 bytes at a time holds the GIL, so on the event loop (or on a thread) a long run
    would starve the heartbeats, and the reaper would take the job away mid-run. A test
    proves the pool version keeps its lease. It has no side effects, so it needs no ledger.
    """
    rounds = int(job.payload.get("rounds", 1000))
    digest = job.job_id.encode()
    for _ in range(rounds):
        digest = hashlib.sha256(digest).digest()
    return {"rounds": rounds, "digest": digest.hex()}
