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
        description="Seconds between lease extensions of a running job; at most half the lease.",
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

    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR"] = Field(
        default="INFO",
        description="INFO logs lifecycle events only; per-job lines are DEBUG.",
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
        # At least two heartbeats per lease, so one slow or lost heartbeat doesn't
        # expire the lease of a healthy job (ADR-025).
        if 2 * self.heartbeat_interval > self.visibility_timeout:
            raise ValueError(
                f"heartbeat_interval ({self.heartbeat_interval}s) must be at most half of "
                f"visibility_timeout ({self.visibility_timeout}s)"
            )
        # A live worker touches its consumer at least every block_ms (each XREADGROUP),
        # so a prune threshold below that would churn live consumers. Safety never depends
        # on this: pruning only ever deletes consumers that own no entries (ADR-029).
        if self.consumer_prune_idle * 1000 <= self.block_ms:
            raise ValueError("consumer_prune_idle must exceed block_ms")
        if self.done_ttl_seconds and self.done_ttl_seconds < _TTL_MARGIN * self.job_lifetime_bound:
            raise ValueError(
                f"done_ttl_seconds ({self.done_ttl_seconds}) must be 0 or at least "
                f"{_TTL_MARGIN}x the redelivery bound of {self.job_lifetime_bound:.0f}s (ADR-010)"
            )
        return self

    @property
    def job_lifetime_bound(self) -> float:
        """Upper bound (s) on how long copies of a job can keep being redelivered without
        anyone heartbeating: every attempt can be delivered `max_deliveries` times, each
        after a full lease, and waits up to `job_backoff_cap` before the next attempt.
        The done/ledger TTL must outlast this, or a late copy would commit twice (ADR-010).
        Time spent waiting undelivered in a backlog is NOT covered: size TTLs for that.
        """
        per_attempt = self.max_deliveries * self.visibility_timeout + self.job_backoff_cap
        return self.max_attempts * per_attempt


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
