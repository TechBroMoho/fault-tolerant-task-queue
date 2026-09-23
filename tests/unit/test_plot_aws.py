"""bench/plot.py's AWS mode: which numbers it takes from the Phase 8 reports (pure logic;
the chart itself is checked by eye)."""

import json
from pathlib import Path
from typing import Any

from bench.plot import aws_rows


def _report(label: str, workers: int, per_s: float, **meta: Any) -> dict[str, Any]:
    return {
        "meta": {"label": label, "workers": workers, "git": "abc", **meta},
        "throughput": {"offered_per_s": per_s, "completed_per_s": per_s},
        "redis": {"main_thread_busy": 0.5},
        "e2e_latency_ms": {"p50": 1, "p99": 2},
        "producers": {"rejected": 0, "blocked": 0, "hosts": {"reported": 2, "expected": 2}},
        "exactly_once": {"ok": True},
    }


def test_rows_come_from_reports_not_snapshots_and_say_which_were_recovered(
    tmp_path: Path,
) -> None:
    (tmp_path / "scaling").mkdir()
    (tmp_path / "backpressure").mkdir()
    for f, r in {
        "scaling/w01.json": _report("w01", 1, 3564.4),
        "backpressure/w12_reject.json": _report("w12_reject", 12, 17783.9, recovered="x"),
    }.items():
        (tmp_path / f).write_text(json.dumps(r))
    # A snapshot with no report (a point that crashed) must not become a row.
    (tmp_path / "backpressure" / "w12_block.services.json").write_text('{"services": []}')
    rows = aws_rows(tmp_path)
    assert [(r["suite"], r["label"], r["completed_per_s"], r["recovered"]) for r in rows] == [
        ("scaling", "w01", 3564, False),
        ("backpressure", "w12_reject", 17784, True),
    ]
    assert rows[0]["hosts"] == "2/2" and rows[0]["exactly_once"] is True
