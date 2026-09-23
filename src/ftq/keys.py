"""Redis key names for one queue.

Every key contains the hash tag `{<queue>}`. Redis Cluster hashes only the text inside
the first `{...}`, so all of a queue's keys land in one slot, which is what lets a Lua
script touch the stream, the done key, and the logs atomically on a cluster. We don't
run Cluster (SPEC §2), but the naming costs nothing and keeps that door open (ADR-018).
"""

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class Keys:
    queue: str

    @property
    def prefix(self) -> str:
        return f"ftq:{{{self.queue}}}"

    @property
    def stream(self) -> str:
        """The job stream. XLEN = undelivered + in-flight, because every exit XDELs."""
        return f"{self.prefix}:stream"

    @property
    def delayed(self) -> str:
        """Sorted set of jobs waiting to be retried, scored by due time (ms, Redis clock)."""
        return f"{self.prefix}:delayed"

    @property
    def dead(self) -> str:
        """Dead-letter stream: jobs that ended DEAD, with why (dlq_* fields)."""
        return f"{self.prefix}:dead"

    @property
    def results(self) -> str:
        """Append-only log: one entry per *first* commit. The chaos verifier counts it."""
        return f"{self.prefix}:results"

    @property
    def effects(self) -> str:
        """Append-only log: one entry per effect the ledger let through."""
        return f"{self.prefix}:effects"

    @property
    def full(self) -> str:
        """Backpressure flag: exists while the queue is "full" (between crossing the high
        watermark and falling below the low one). Hysteresis state, set by enqueue.lua."""
        return f"{self.prefix}:full"

    @property
    def stats(self) -> str:
        """Hash of counters (metrics.py)."""
        return f"{self.prefix}:stats"

    def done(self, job_id: str) -> str:
        """Hash holding the job's terminal state and result."""
        return f"{self.prefix}:done:{job_id}"

    def idem(self, idempotency_key: str) -> str:
        """Enqueue idempotency key -> original job_id."""
        return f"{self.prefix}:idem:{idempotency_key}"

    def ledger(self, effect_key: str) -> str:
        """EffectLedger marker: exists iff the effect with this key was applied."""
        return f"{self.prefix}:ledger:{effect_key}"
