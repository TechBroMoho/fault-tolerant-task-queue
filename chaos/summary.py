"""A Markdown table of saved chaos reports, one row per report, for docs/RESULTS.md.

    uv run python -m chaos.summary results/ci/chaos_report_*_N1M_*.json

Nothing is computed beyond reading fields: every cell is a value from the report the row
names, so the table can be regenerated and diffed against what RESULTS.md shows.
"""

import json
import sys
from pathlib import Path
from typing import Any

COLUMNS = [
    "report", "git", "date (UTC)", "jobs", "workers", "seed", "result", "SUCCEEDED", "DEAD",
    "I1", "I2", "I2b", "I3", "I4", "I5", "kills", "pauses", "net faults", "crash restarts",
    "reclaimed", "dup. results suppressed", "dup. effects suppressed", "max delivery*", "s",
]  # fmt: skip


def row(path: Path) -> list[str]:
    r: dict[str, Any] = json.loads(path.read_text())
    run, v = r["run"], r["verifier"]
    inv, counters = v["invariants"], v["counters"]
    seen = inv["I4_faults_happened"]["observed"]
    states = inv["I1_no_loss"]["states"]

    def ok(name: str) -> str:
        return "ok" if inv[name]["ok"] else "FAIL"

    return [
        path.name, run["git"], run["date_utc"][:16].replace("T", " "), f"{run['accepted']:,}",
        str(run["workers"]), str(run["seed"]), "PASS" if v["passed"] else "FAIL",
        f"{states.get('SUCCEEDED', 0):,}", f"{states.get('DEAD', 0):,}",
        ok("I1_no_loss"), ok("I2_no_duplicate_effects"), ok("I2b_no_duplicate_results"),
        ok("I3_dlq_correct"), ok("I4_faults_happened"), ok("I5_drained"),
        str(seen["kills"]), str(seen["pauses"]), str(seen["network_windows"]),
        str(seen["crash_restarts"]), f"{counters['reclaimed']:,}",
        f"{counters['duplicates_suppressed']:,}", f"{counters['effects_suppressed']:,}",
        str(v["max_delivery_excluding_crashy"]), f"{run['seconds']['total']:,.0f}",
    ]  # fmt: skip


def table(paths: list[Path]) -> str:
    lines = ["| " + " | ".join(COLUMNS) + " |", "|" + "---|" * len(COLUMNS)]
    lines += ["| " + " | ".join(row(p)) + " |" for p in paths]
    return "\n".join(lines) + "\n"


def main() -> None:
    sys.stdout.write(table([Path(a) for a in sys.argv[1:]]))
    sys.stdout.write("\n*max delivery: the highest delivery count of any non-crashy job.\n")


if __name__ == "__main__":
    main()
