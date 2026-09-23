"""Structured logs: one JSON object per line, carrying job_id, attempt, and worker_id.

Why JSON: on AWS the logs land in CloudWatch, and in the chaos runs they are the first
thing to search when an invariant fails ("every line for job X across all workers").
A field is searchable; a value buried in a sentence is not.

Why the default level logs so little: at 10K jobs/s, one INFO line per job would drown
CI output and cost real money in CloudWatch ingestion (SPEC Phase 3). So INFO is
lifecycle only (started, draining, stopped, pool replaced). A job's normal path is
DEBUG. Anything unusual about a job (DLQ, timeout, lost lease) is WARNING, and is rare
by construction.

Context goes in `extra=`, never in the message text, so the message stays constant
and greppable: `log.warning("job timed out", extra={"job_id": ..., "attempt": ...})`.
"""

import json
import logging
import sys
from collections.abc import Mapping, MutableMapping
from datetime import UTC, datetime
from typing import Any, Literal

# Fields copied from a record's `extra` into the output, in this order.
CONTEXT_FIELDS = ("worker_id", "job_id", "job_type", "attempt", "entry_id")


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        out: dict[str, Any] = {
            "ts": datetime.fromtimestamp(record.created, UTC).isoformat(timespec="milliseconds"),
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
        }
        for name in CONTEXT_FIELDS:
            if hasattr(record, name):
                out[name] = getattr(record, name)
        if record.exc_info:
            out["exc"] = self.formatException(record.exc_info)
        return json.dumps(out, default=str)


class TextFormatter(logging.Formatter):
    """For humans at a terminal: the classic line, then the context as key=value."""

    def __init__(self) -> None:
        super().__init__("%(asctime)s %(levelname)s %(name)s: %(message)s")

    def format(self, record: logging.LogRecord) -> str:
        line = super().format(record)
        context = [f"{n}={getattr(record, n)}" for n in CONTEXT_FIELDS if hasattr(record, n)]
        return f"{line} [{' '.join(context)}]" if context else line


def configure(level: str, fmt: Literal["json", "text"]) -> None:
    """Send the root logger to stderr in the chosen format (replaces earlier handlers)."""
    handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(JsonFormatter() if fmt == "json" else TextFormatter())
    root = logging.getLogger()
    root.handlers[:] = [handler]
    root.setLevel(level)


class ContextLogger(logging.LoggerAdapter[logging.Logger]):
    """Adds fixed context (e.g. worker_id) to every record, merged with per-call `extra`.

    (The stdlib adapter in 3.12 replaces a call's `extra` with its own instead of
    merging; `merge_extra=True` only arrives in 3.13.)
    """

    def process(
        self, msg: Any, kwargs: MutableMapping[str, Any]
    ) -> tuple[Any, MutableMapping[str, Any]]:
        call_extra: Mapping[str, Any] = kwargs.get("extra") or {}
        kwargs["extra"] = {**(self.extra or {}), **call_extra}
        return msg, kwargs
