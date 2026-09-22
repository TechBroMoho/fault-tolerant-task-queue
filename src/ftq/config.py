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
        # The full ADR-010 relation (attempts x backoff + deliveries x lease + margin)
        # needs the Phase 2 lease/retry knobs; it is validated once those exist.
        return self


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
