"""Structured log output (pure logic): the fields a chaos post-mortem searches by."""

import json
import logging

from ftq.logs import ContextLogger, JsonFormatter, TextFormatter


def _record(extra: dict[str, object]) -> logging.LogRecord:
    captured: list[logging.LogRecord] = []

    class Capture(logging.Handler):
        def emit(self, record: logging.LogRecord) -> None:
            captured.append(record)

    logger = logging.getLogger("ftq.test-logs")
    logger.propagate = False
    logger.handlers[:] = [Capture()]
    logger.setLevel(logging.DEBUG)
    ContextLogger(logger, {"worker_id": "w-1"}).warning("job %s", "timed out", extra=extra)
    return captured[0]


def test_json_lines_carry_worker_job_and_attempt() -> None:
    record = _record({"job_id": "j-9", "attempt": 2, "job_type": "send_email"})
    out = json.loads(JsonFormatter().format(record))
    assert out["msg"] == "job timed out"
    assert (out["level"], out["logger"]) == ("WARNING", "ftq.test-logs")
    assert (out["worker_id"], out["job_id"], out["attempt"], out["job_type"]) == (
        "w-1",
        "j-9",
        2,
        "send_email",
    )


def test_adapter_merges_rather_than_replaces_call_extra() -> None:
    # The stdlib adapter in 3.12 would drop the per-call job fields; ours keeps both.
    record = _record({"job_id": "j-1"})
    assert (getattr(record, "worker_id"), getattr(record, "job_id")) == ("w-1", "j-1")  # noqa: B009


def test_text_format_appends_context() -> None:
    line = TextFormatter().format(_record({"job_id": "j-1", "attempt": 0}))
    assert line.endswith("job timed out [worker_id=w-1 job_id=j-1 attempt=0]")
