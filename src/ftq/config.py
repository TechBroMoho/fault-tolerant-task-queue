"""Every tunable knob, read from `FTQ_*` environment variables (SPEC §8).

Timeouts and retries are set explicitly instead of inheriting redis-py defaults, which
changed in redis-py 8 and could change again (DECISIONS.md ADR-006). A black-holed
connection must fail within `socket_timeout`, not hang a worker forever.
"""

import re
from typing import Literal, Self

import redis.asyncio as aioredis
from pydantic import Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict
from redis.asyncio.retry import Retry
from redis.backoff import ExponentialWithJitterBackoff

# Queue names go inside `{...}` hash tags in key names (keys.py), so braces are forbidden
# and the charset is kept boring enough to be safe in keys, logs, and shell commands.
_QUEUE_NAME = re.compile(r"^[A-Za-z0-9_.-]{1,64}$")

# done_ttl_seconds must be at least this many times Settings.job_lifetime_bound (ADR-010).
_TTL_MARGIN = 10


class Settings(BaseSettings):
    """Configuration for producers and workers. Field docs double as the README table."""

    model_config = SettingsConfigDict(env_prefix="FTQ_", frozen=True)

    # ------------------------------------------------------------ connection
    redis_url: str = Field(
        default="redis://localhost:6379/0",
        description="Redis connection URL.",
    )
    socket_timeout: float = Field(
        default=5.0,
        gt=0,
        description="Seconds to wait for any single Redis reply before raising TimeoutError.",
    )
    socket_connect_timeout: float = Field(
        default=2.0, gt=0, description="Seconds to wait for a TCP connect to Redis."
    )
    health_check_interval: float = Field(
        default=10.0,
        ge=0,
        description="Ping idle pooled connections older than this (s) before reuse; 0 disables.",
    )
    retry_attempts: int = Field(
        default=3,
        ge=0,
        description="Client-side retries of a command after a connection error or timeout.",
    )
    retry_backoff_base: float = Field(
        default=0.05, gt=0, description="Base (s) of the jittered exponential retry backoff."
    )
    retry_backoff_cap: float = Field(
        default=1.0, gt=0, description="Cap (s) on a single retry backoff sleep."
    )

    # ------------------------------------------------------------ queue
    queue: str = Field(default="default", description="Queue name (one stream + group each).")
    group: str = Field(default="workers", description="Consumer group name within the queue.")

    # ------------------------------------------------------------ worker
    concurrency: int = Field(
        default=10,
        ge=1,
        description="Max jobs in flight per worker process (also the XREADGROUP COUNT cap).",
    )
    block_ms: int = Field(
        default=1000,
        ge=1,
        description="XREADGROUP BLOCK timeout (ms). Bounds how long a stop request waits.",
    )
    shutdown_grace: float = Field(
        default=30.0,
        ge=0,
        description="On SIGTERM, seconds to let in-flight jobs finish before abandoning them.",
    )

    process_pool_size: int = Field(
        default=2,
        ge=1,
        description=(
            "Processes for CPU-bound handlers registered with pool='process' (ADR-028). "
            "Thread-pool handlers get `concurrency` threads."
        ),
    )
    job_timeout: float = Field(
        default=300.0,
        gt=0,
        description=(
            "Seconds one handler run may take before it counts as a failed attempt "
            "(ADR-030). A handler type can override it at registration."
        ),
    )

    # ------------------------------------------------------------ leases (ADR-023, ADR-025)
    visibility_timeout: float = Field(
        default=30.0,
        gt=0,
        description=(
            "Lease length (s): an entry idle this long in the PEL is reclaimed by XAUTOCLAIM."
        ),
    )
    heartbeat_interval: float = Field(
        default=10.0,
        gt=0,
        description="Seconds between lease extensions of a running job; at most lease / 3.",
    )
    reap_interval: float = Field(
        default=5.0,
        gt=0,
        description="Seconds between reaper passes (XAUTOCLAIM of expired leases).",
    )

    # ------------------------------------------------------------ retries and DLQ (ADR-026/027)
    max_attempts: int = Field(
        default=5,
        ge=1,
        description="Handler runs (first try + retries) before a failing job goes to the DLQ.",
    )
    max_deliveries: int = Field(
        default=10,
        ge=1,
        description=(
            "Deliveries of one stream entry (XPENDING count) before it goes to the DLQ "
            "unrun: the job keeps crashing its worker."
        ),
    )
    suspect_deliveries: int = Field(
        default=3,
        ge=2,
        description=(
            "A reclaimed entry at this delivery count or more is suspected of crashing its "
            "workers; each worker runs at most one suspect at a time, so jobs that ran "
            "beside a crashing job don't follow it to the DLQ (ADR-035)."
        ),
    )
    job_backoff_base: float = Field(
        default=1.0,
        gt=0,
        description="Retry delay base (s): delay = random(0, min(cap, base * 2^attempt)).",
    )
    job_backoff_cap: float = Field(
        default=300.0, gt=0, description="Cap (s) on a single retry delay."
    )
    scheduler_interval: float = Field(
        default=0.5,
        gt=0,
        description="Seconds between moves of due retries from the delayed set to the stream.",
    )
    scheduler_batch: int = Field(
        default=500, ge=1, description="Max due retries moved per scheduler pass."
    )

    # ------------------------------------------------------------ consumer cleanup (ADR-029)
    consumer_prune_idle: float = Field(
        default=3600.0,
        gt=0,
        description=(
            "Delete a group consumer idle this long (s), but ONLY if it owns zero pending "
            "entries (deleting one that does would drop its jobs from the PEL)."
        ),
    )
    consumer_prune_interval: float = Field(
        default=60.0, gt=0, description="Seconds between consumer-cleanup passes."
    )

    # ------------------------------------------------------------ backpressure (ADR-031)
    backpressure_mode: Literal["reject", "block"] = Field(
        default="reject",
        description=(
            "When the queue is full: 'reject' raises QueueFull at once; 'block' waits "
            "(up to block_timeout) for the depth to fall below the low watermark."
        ),
    )
    high_watermark: int = Field(
        default=100_000,
        ge=1,
        description=(
            "Queue depth (stream length + delayed retries) at which enqueue stops accepting jobs."
        ),
    )
    low_watermark: int = Field(
        default=80_000,
        ge=0,
        description="Once full, enqueue accepts jobs again only when depth falls below this.",
    )
    block_timeout: float = Field(
        default=30.0,
        ge=0,
        description="In 'block' mode, seconds to wait for room before raising QueueFull.",
    )
    block_poll_interval: float = Field(
        default=0.05,
        gt=0,
        description="In 'block' mode, seconds between admission retries while full.",
    )

    # ------------------------------------------------------------ logging
    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR"] = Field(
        default="INFO",
        description="INFO logs lifecycle events only; per-job lines are DEBUG.",
    )
    log_format: Literal["json", "text"] = Field(
        default="json",
        description="'json': one object per line with job_id/attempt/worker_id fields.",
    )

    # ------------------------------------------------------------ retention
    done_ttl_seconds: int = Field(
        default=7 * 24 * 3600,
        ge=0,
        description=(
            "TTL of done/ledger keys; 0 = never expire (chaos and tests). Must outlast any "
            "possible redelivery of the job (ADR-010)."
        ),
    )
    idempotency_ttl_seconds: int = Field(
        default=24 * 3600,
        ge=1,
        description="How long an enqueue idempotency key maps to its original job_id.",
    )

    @model_validator(mode="after")
    def _check(self) -> Self:
        if not _QUEUE_NAME.match(self.queue):
            raise ValueError(f"queue must match {_QUEUE_NAME.pattern}, got {self.queue!r}")
        # A blocking XREADGROUP holds the socket open for block_ms. If that reaches
        # socket_timeout, every idle poll would raise TimeoutError and be retried.
        if self.block_ms / 1000 >= self.socket_timeout:
            raise ValueError(
                f"block_ms ({self.block_ms}) must be below socket_timeout "
                f"({self.socket_timeout}s) or idle polls time out"
            )
        if self.retry_backoff_base > self.retry_backoff_cap:
            raise ValueError("retry_backoff_base must not exceed retry_backoff_cap")
        if self.job_backoff_base > self.job_backoff_cap:
            raise ValueError("job_backoff_base must not exceed job_backoff_cap")
        # Beats land every interval + one round trip. If one is lost, idle reaches
        # 2 x (interval + RTT) before the next lands, so at interval = lease/2 a single
        # lost beat would already let the reaper take a healthy job. A third of the lease
        # leaves a full interval of margin for one lost or slow beat (ADR-025).
        if 3 * self.heartbeat_interval > self.visibility_timeout:
            raise ValueError(
                f"heartbeat_interval ({self.heartbeat_interval}s) must be at most a third of "
                f"visibility_timeout ({self.visibility_timeout}s)"
            )
        # A live worker touches its consumer at least every block_ms (each XREADGROUP),
        # so a prune threshold below that would churn live consumers. Safety never depends
        # on this: pruning only ever deletes consumers that own no entries (ADR-029).
        if self.consumer_prune_idle * 1000 <= self.block_ms:
            raise ValueError("consumer_prune_idle must exceed block_ms")
        # Hysteresis needs a gap: with low == high, the queue would flip between full and
        # not full on every enqueue at the boundary (ADR-031).
        if self.low_watermark >= self.high_watermark:
            raise ValueError(
                f"low_watermark ({self.low_watermark}) must be below high_watermark "
                f"({self.high_watermark})"
            )
        self.check_ttl_covers(self.job_timeout)
        return self

    def lifetime_bound(self, timeout: float) -> float:
        """Upper bound (s) on how long copies of a job whose runs time out after
        `timeout` can keep being delivered. One delivery ends at the latest when its run
        times out (then it's retried), or, if the worker dies first, a lease later, when
        it's reclaimed: at most `timeout + visibility_timeout`. Every attempt can be
        delivered `max_deliveries` times and then waits up to `job_backoff_cap`.

        The done/ledger TTL must outlast this, or a late copy would commit twice (ADR-010).
        Before the per-job timeout (ADR-030), a heartbeating job had no bound at all.
        Time spent waiting undelivered in a backlog is still NOT covered: size TTLs for it.
        """
        per_delivery = timeout + self.visibility_timeout
        per_attempt = self.max_deliveries * per_delivery + self.job_backoff_cap
        return self.max_attempts * per_attempt

    @property
    def suspect_threshold(self) -> int:
        """The delivery count from which a reclaimed entry is a suspect (ADR-035):
        `suspect_deliveries`, but never above `max_deliveries`. Otherwise, with a low
        max_deliveries, entries would reach the DLQ before they could ever be suspects,
        and a crashing job's companions would follow it there again."""
        return min(self.suspect_deliveries, self.max_deliveries)

    @property
    def job_lifetime_bound(self) -> float:
        """`lifetime_bound` for the default `job_timeout`."""
        return self.lifetime_bound(self.job_timeout)

    def check_ttl_covers(self, timeout: float) -> None:
        """Raise ValueError unless done_ttl_seconds is 0 or >= 10x lifetime_bound(timeout).
        The worker also calls this for each handler type's own timeout."""
        bound = self.lifetime_bound(timeout)
        if self.done_ttl_seconds and self.done_ttl_seconds < _TTL_MARGIN * bound:
            raise ValueError(
                f"done_ttl_seconds ({self.done_ttl_seconds}) must be 0 or at least "
                f"{_TTL_MARGIN}x the redelivery bound of {bound:.0f}s for a {timeout}s "
                "job timeout (ADR-010)"
            )


def make_redis(settings: Settings) -> aioredis.Redis:
    """Build the asyncio Redis client every ftq component uses.

    Retries are safe because every write we send is idempotent on re-send (ADR-006):
    enqueue dedups by idempotency key or, without one, by job_id at commit time; the
    commit and ledger scripts are first-wins.
    """
    retry = Retry(
        ExponentialWithJitterBackoff(
            cap=settings.retry_backoff_cap, base=settings.retry_backoff_base
        ),
        settings.retry_attempts,
    )
    return aioredis.Redis.from_url(
        settings.redis_url,
        decode_responses=True,
        socket_timeout=settings.socket_timeout,
        socket_connect_timeout=settings.socket_connect_timeout,
        health_check_interval=settings.health_check_interval,
        retry=retry,
    )
