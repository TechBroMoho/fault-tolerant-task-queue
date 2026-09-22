"""Handler registry: maps a job `type` string to the code that runs it, and how.

Two kinds of handler (ADR-028):

- **async** (`register`): a coroutine that runs on the worker's event loop. It gets a
  `JobContext`, so it can perform effects through the ledger. It must not block: the
  loop also runs the heartbeats, and a handler that holds it past the lease lets the
  reaper take the job away.
- **sync** (`register_sync`): a plain function that runs in a thread pool (blocking I/O,
  e.g. a synchronous SDK) or a process pool (CPU-bound work). It gets the `Job` only,
  a picklable value, and returns a result. Heartbeats keep running on the loop meanwhile.
  It has no ledger, because the ledger is an async Redis client that belongs to the
  loop. A job that computes AND performs an effect is written as an async handler that
  runs its computation with `asyncio.to_thread` (or its own executor) and then awaits
  the ledger, or is split into two jobs.

`heartbeat=False` opts a handler out of lease extension. The chaos test's "slow" jobs use
it so their leases are guaranteed to expire (ADR-007).
"""

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any, Literal

from ftq.ledger import EffectLedger
from ftq.models import Job


@dataclass(frozen=True, slots=True)
class JobContext:
    """What an async handler receives. Side effects go through `ledger` (effectively-once)."""

    job: Job
    ledger: EffectLedger
    worker_id: str


# Both kinds return a JSON-serializable result, stored with the job's done state.
AsyncHandler = Callable[[JobContext], Awaitable[Any]]
SyncHandler = Callable[[Job], Any]
Pool = Literal["thread", "process"]


@dataclass(frozen=True, slots=True)
class AsyncSpec:
    fn: AsyncHandler
    heartbeat: bool


@dataclass(frozen=True, slots=True)
class SyncSpec:
    fn: SyncHandler
    pool: Pool
    heartbeat: bool


HandlerSpec = AsyncSpec | SyncSpec


class Registry:
    def __init__(self) -> None:
        self._handlers: dict[str, HandlerSpec] = {}

    def register(
        self, job_type: str, *, heartbeat: bool = True
    ) -> Callable[[AsyncHandler], AsyncHandler]:
        """Decorator for an async handler: `@registry.register("send_email")`."""

        def decorator(fn: AsyncHandler) -> AsyncHandler:
            self._add(job_type, AsyncSpec(fn, heartbeat))
            return fn

        return decorator

    def register_sync(
        self, job_type: str, *, pool: Pool, heartbeat: bool = True
    ) -> Callable[[SyncHandler], SyncHandler]:
        """Decorator for a blocking handler run in a thread or process pool."""

        def decorator(fn: SyncHandler) -> SyncHandler:
            # A process pool pickles the function by its import path, so it must be a
            # module-level function. Fail at registration, not on the first job.
            if pool == "process" and "<" in fn.__qualname__:
                raise ValueError(
                    f"process-pool handler {fn.__qualname__!r} must be a module-level function"
                )
            self._add(job_type, SyncSpec(fn, pool, heartbeat))
            return fn

        return decorator

    def _add(self, job_type: str, spec: HandlerSpec) -> None:
        if job_type in self._handlers:
            raise ValueError(f"handler for {job_type!r} is already registered")
        self._handlers[job_type] = spec

    def get(self, job_type: str) -> HandlerSpec | None:
        return self._handlers.get(job_type)

    def types(self) -> list[str]:
        return sorted(self._handlers)
