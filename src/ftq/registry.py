"""Handler registry: maps a job `type` string to the coroutine that runs it."""

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any

from ftq.ledger import EffectLedger
from ftq.models import Job


@dataclass(frozen=True, slots=True)
class JobContext:
    """What a handler receives. Side effects go through `ledger` (effectively-once)."""

    job: Job
    ledger: EffectLedger
    worker_id: str


# A handler returns a JSON-serializable result, stored with the job's done state.
Handler = Callable[[JobContext], Awaitable[Any]]


class Registry:
    def __init__(self) -> None:
        self._handlers: dict[str, Handler] = {}

    def register(self, job_type: str) -> Callable[[Handler], Handler]:
        """Decorator: `@registry.register("send_email")`."""

        def decorator(handler: Handler) -> Handler:
            if job_type in self._handlers:
                raise ValueError(f"handler for {job_type!r} is already registered")
            self._handlers[job_type] = handler
            return handler

        return decorator

    def get(self, job_type: str) -> Handler | None:
        return self._handlers.get(job_type)

    def types(self) -> list[str]:
        return sorted(self._handlers)
