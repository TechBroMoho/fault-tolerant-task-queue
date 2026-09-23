"""Charts and a summary table from the saved benchmark reports.

    uv run python -m bench.plot [--results results/local/bench]

Reads every `<suite>/*.json` that bench/run.py wrote and produces, next to them:

- `scaling.png`: completed jobs/s vs workers (median of the repeats, min-max bars),
  with the linear extrapolation of one worker for reference;
- `bottleneck.png`: what each component used at each fleet size: Redis's main thread,
  the busiest worker, the load generator (each a fraction of one core), and the whole
  Docker VM (busy CPUs of how many it has);
- `concurrency.png`: one worker's throughput vs FTQ_CONCURRENCY;
- `latency.png`: end-to-end p50/p95/p99 vs offered load;
- `backpressure.png`: queue depth against the watermarks, and offered vs accepted per
  second, in reject and block mode;
- `summary.json` / `summary.md`: every number the charts show, per point.

Nothing is computed here that isn't in the reports: this only selects and draws.
"""

import argparse
import json
import statistics
from collections import defaultdict
from pathlib import Path
from typing import Any

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt  # (after the backend is chosen)

DEFAULT = Path(__file__).resolve().parents[1] / "results" / "local" / "bench"

# Reference categorical palette, fixed order (blue, orange, aqua, yellow), and ink.
BLUE, ORANGE, AQUA, YELLOW = "#2a78d6", "#eb6834", "#1baf7a", "#eda100"
INK, INK_2, MUTED, GRID, SURFACE = "#0b0b0b", "#52514e", "#8a8983", "#e4e3de", "#fcfcfb"
SUBTITLE = "Local run: MacBook (Apple M4), Docker Desktop VM with 10 CPUs. Not the AWS headline."


def _style() -> None:
    plt.rcParams.update(
        {
            "figure.facecolor": SURFACE,
            "axes.facecolor": SURFACE,
            "axes.edgecolor": GRID,
            "axes.labelcolor": INK_2,
            "axes.titlecolor": INK,
            "axes.titlesize": 11,
            "axes.titleweight": "bold",
            "axes.titlelocation": "left",
            "axes.spines.top": False,
            "axes.spines.right": False,
            "axes.grid": True,
            "grid.color": GRID,
            "grid.linewidth": 0.8,
            "xtick.color": INK_2,
            "ytick.color": INK_2,
            "legend.frameon": False,
            "font.size": 9,
            "lines.linewidth": 2,
        }
    )


def load(results: Path, suite: str) -> list[dict[str, Any]]:
    return [json.loads(f.read_text()) for f in sorted((results / suite).glob("*.json"))]


def _finish(fig: Any, title: str, out: Path) -> None:
    fig.suptitle(title, x=0.01, ha="left", fontsize=13, fontweight="bold", color=INK)
    fig.text(0.01, 0.935, SUBTITLE, ha="left", fontsize=8.5, color=INK_2)
    fig.tight_layout(rect=(0, 0, 1, 0.92))
    fig.savefig(out, dpi=150)
    plt.close(fig)
    print(f"wrote {out}")


def _row(r: dict[str, Any]) -> dict[str, Any]:
    """The per-point numbers the charts and the table use."""
    cpu, lat, t = r["cpu"], r["e2e_latency_ms"], r["throughput"]
    return {
        "label": r["meta"]["label"],
        "workers": r["meta"]["workers"],
        "concurrency": r["meta"]["worker_concurrency"],
        "offered_per_s": t["offered_per_s"],
        "accepted_per_s": t["accepted_per_s"],
        "completed_per_s": t["completed_per_s"],
        "e2e_p50_ms": lat.get("p50"),
        "e2e_p95_ms": lat.get("p95"),
        "e2e_p99_ms": lat.get("p99"),
        "enqueue_call_p50_ms": r["enqueue_call_ms"].get("p50"),
        "enqueue_call_p99_ms": r["enqueue_call_ms"].get("p99"),
        "enqueue_batch_p50": r["enqueue_batch_jobs"].get("p50"),
        "redis_main_thread": r["redis"]["main_thread_busy"],
        "redis_container": cpu["redis_container"],
        "worker_mean": cpu["worker_mean"],
        "worker_max": cpu["worker_max"],
        "workers_sum": cpu["workers_sum"],
        "loadgen": cpu["loadgen_container"],
        "vm_busy_cpus": cpu["vm_busy_cpus"],
        "vm_cpus": cpu["vm_cpus"],
        "depth_min": r["depth_in_window"]["min"],
        "depth_max": r["depth_in_window"]["max"],
        "rejected": r["producers"]["rejected"],
        "blocked": r["producers"]["blocked"],
        "blocked_seconds": r["producers"]["blocked_seconds"],
        "exactly_once": r["exactly_once"]["ok"],
        "duplicate_results": r["exactly_once"]["duplicate_results"],
        "missing": r["exactly_once"]["missing"],
        "worker_log_lines": r["worker_log_lines"],
    }


def _by_workers(rows: list[dict[str, Any]]) -> dict[int, list[dict[str, Any]]]:
    groups: dict[int, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        groups[row["workers"]].append(row)
    return dict(sorted(groups.items()))


def _median(rows: list[dict[str, Any]], key: str) -> float:
    values: list[float] = [r[key] for r in rows if r[key] is not None]
    return statistics.median(values)


def plot_scaling(rows: list[dict[str, Any]], out: Path) -> None:
    groups = _by_workers(rows)
    ws = list(groups)
    med = [_median(g, "completed_per_s") for g in groups.values()]
    lo = [
        m - min(r["completed_per_s"] for r in g) for m, g in zip(med, groups.values(), strict=True)
    ]
    hi = [
        max(r["completed_per_s"] for r in g) - m for m, g in zip(med, groups.values(), strict=True)
    ]
    fig, ax = plt.subplots(figsize=(7.5, 4.6))
    ax.plot(
        ws, [med[0] * w for w in ws], color=MUTED, lw=1.2, ls="--", label="1 worker x N (linear)"
    )
    ax.errorbar(
        ws,
        med,
        yerr=[lo, hi],
        color=BLUE,
        marker="o",
        ms=7,
        capsize=4,
        mec=SURFACE,
        mew=2,
        label="measured, median of repeats (bars: min-max)",
    )
    for w, m in zip(ws, med, strict=True):
        ax.annotate(
            f"{m / 1000:.1f}K",
            (w, m),
            textcoords="offset points",
            xytext=(0, 10),
            ha="center",
            color=INK,
            fontsize=9,
        )
    ax.set_xticks(ws)
    ax.set_xlabel("worker containers (one process each)")
    ax.set_ylabel("completed jobs/s (steady-state window)")
    ax.set_ylim(0, max(max(med) * 1.35, med[0] * 2.5))
    ax.legend(loc="upper left")
    n = len(next(iter(groups.values())))
    ax.set_title(f"send_email, 100 B payload, {n} repeats per point", fontweight="normal")
    _finish(fig, "Completed jobs/s vs worker count", out)


def plot_bottleneck(rows: list[dict[str, Any]], out: Path) -> None:
    groups = _by_workers(rows)
    ws = list(groups)
    fig, (a1, a2) = plt.subplots(1, 2, figsize=(10, 4.4))
    series = [
        ("redis_main_thread", "Redis main thread", BLUE),
        ("worker_max", "busiest worker", ORANGE),
        ("loadgen", "load generator (all processes)", AQUA),
    ]
    for key, label, color in series:
        ys = [_median(g, key) for g in groups.values()]
        a1.plot(ws, ys, color=color, marker="o", ms=7, mec=SURFACE, mew=2, label=label)
        a1.annotate(
            f"{ys[-1]:.2f}",
            (ws[-1], ys[-1]),
            textcoords="offset points",
            xytext=(8, -3),
            color=INK,
            fontsize=8.5,
        )
    a1.axhline(1.0, color=MUTED, lw=1, ls=":")
    a1.text(ws[0], 1.02, "one full core", color=INK_2, fontsize=8, va="bottom")
    a1.set_ylim(0, 1.25)
    a1.set_xticks(ws)
    a1.set_xlabel("worker containers")
    a1.set_ylabel("CPU, fraction of one core")
    a1.set_title("Per component")
    a1.legend(loc="lower right")

    busy = [_median(g, "vm_busy_cpus") for g in groups.values()]
    cpus = _median(rows, "vm_cpus")
    a2.bar(ws, busy, width=0.8, color=BLUE, label="busy CPUs (whole VM)")
    for w, b in zip(ws, busy, strict=True):
        a2.annotate(
            f"{b:.1f}",
            (w, b),
            textcoords="offset points",
            xytext=(0, 4),
            ha="center",
            color=INK,
            fontsize=8.5,
        )
    a2.axhline(cpus, color=MUTED, lw=1, ls=":")
    a2.text(ws[0] - 0.4, cpus + 0.15, f"{cpus:.0f} CPUs in the VM", color=INK_2, fontsize=8)
    a2.set_ylim(0, cpus * 1.2)
    a2.set_xticks(ws)
    a2.set_xlabel("worker containers")
    a2.set_ylabel("CPUs busy")
    a2.set_title("Whole Docker VM")
    _finish(fig, "Where the time goes (medians over the steady-state window)", out)


def plot_concurrency(rows: list[dict[str, Any]], out: Path) -> None:
    rows = sorted(rows, key=lambda r: r["concurrency"])
    cs = [r["concurrency"] for r in rows]
    fig, ax = plt.subplots(figsize=(7, 4.2))
    ys = [r["completed_per_s"] for r in rows]
    ax.plot(cs, ys, color=BLUE, marker="o", ms=7, mec=SURFACE, mew=2)
    for c, y, r in zip(cs, ys, rows, strict=True):
        ax.annotate(
            f"{y / 1000:.2f}K\n(CPU {r['worker_max']:.2f})",
            (c, y),
            textcoords="offset points",
            xytext=(0, 10),
            ha="center",
            fontsize=8.5,
        )
    ax.set_xscale("log")
    ax.set_xticks(cs, [str(c) for c in cs])
    ax.minorticks_off()
    ax.set_ylim(0, max(ys) * 1.35)
    ax.set_xlabel("FTQ_CONCURRENCY (jobs in flight per worker)")
    ax.set_ylabel("completed jobs/s")
    ax.set_title("One worker container, saturated", fontweight="normal")
    _finish(fig, "One worker's throughput vs its in-flight cap", out)


def plot_latency(rows: list[dict[str, Any]], out: Path) -> None:
    rows = sorted(rows, key=lambda r: r["offered_per_s"])
    xs = [r["completed_per_s"] for r in rows]
    fig, ax = plt.subplots(figsize=(7.5, 4.6))
    for key, label, color in [
        ("e2e_p50_ms", "p50", BLUE),
        ("e2e_p95_ms", "p95", ORANGE),
        ("e2e_p99_ms", "p99", AQUA),
    ]:
        ys = [r[key] for r in rows]
        ax.plot(xs, ys, color=color, marker="o", ms=7, mec=SURFACE, mew=2, label=label)
        ax.annotate(
            f"{label} {ys[-1]:g} ms",
            (xs[-1], ys[-1]),
            textcoords="offset points",
            xytext=(8, -3),
            color=INK,
            fontsize=8.5,
        )
    ax.set_yscale("log")
    ax.set_xlabel("throughput, completed jobs/s (= offered: below capacity)")
    ax.set_ylabel("enqueue -> commit latency, ms (log; Redis TIME, 1 ms resolution)")
    ax.set_xlim(0, max(xs) * 1.2)
    ax.legend(loc="upper left")
    w = rows[0]["workers"]
    ax.set_title(f"{w} workers, open-loop offered load below capacity", fontweight="normal")
    _finish(fig, "End-to-end latency vs load", out)


def plot_backpressure(reports: list[dict[str, Any]], out: Path) -> None:
    fig, axes = plt.subplots(len(reports), 2, figsize=(11, 3.6 * len(reports)), squeeze=False)
    for (a_depth, a_rate), r in zip(axes, reports, strict=True):
        mode = r["spec"]["backpressure"]
        t0 = r["t0_ms"]
        samples = r["timeline"]["samples"]
        ts = [(s["t_ms"] - t0) / 1000 for s in samples]
        a_depth.plot(ts, [s["depth"] for s in samples], color=BLUE, label="queue depth")
        for mark, name in [
            (r["spec"]["high_watermark"], "high"),
            (r["spec"]["low_watermark"], "low"),
        ]:
            a_depth.axhline(mark, color=MUTED, lw=1, ls=":")
            a_depth.text(ts[-1], mark, f" {name} watermark", color=INK_2, fontsize=8, va="center")
        a_depth.set_ylim(0, r["spec"]["high_watermark"] * 1.3)
        a_depth.set_xlabel("seconds since the load started")
        a_depth.set_ylabel("depth (stream + delayed)")
        a_depth.set_title(f"{mode} mode: depth stays under the high watermark")

        per_s = r["timeline"]["producer_per_s"]
        secs = sorted(int(s) for s in per_s)
        done = r["timeline"]["completed_per_s"]
        a_rate.plot(
            secs, [per_s[str(s)][0] for s in secs], color=MUTED, lw=1.5, ls="--", label="offered"
        )
        a_rate.plot(secs, [per_s[str(s)][1] for s in secs], color=ORANGE, label="accepted")
        a_rate.plot(secs, [done.get(str(s), 0) for s in secs], color=AQUA, label="completed")
        a_rate.set_ylim(0, max(v[0] for v in per_s.values()) * 1.3)
        a_rate.set_xlabel("seconds since the load started")
        a_rate.set_ylabel("jobs per second")
        p = r["producers"]
        extra = (
            f"rejected {p['rejected']:,}"
            if mode == "reject"
            else f"blocked {p['blocked']:,} jobs, {p['blocked_seconds']:.0f} s waiting; "
            f"rejected {p['rejected']:,}"
        )
        a_rate.set_title(f"{mode} mode: intake throttled to capacity ({extra})", fontsize=9.5)
        a_rate.legend(loc="lower right", ncols=3)
    _finish(fig, "Backpressure: offered load 1.5x capacity", out)


def _table(rows: list[dict[str, Any]], cols: list[str]) -> str:
    lines = ["| " + " | ".join(cols) + " |", "|" + "---|" * len(cols)]
    for r in rows:
        lines.append("| " + " | ".join(str(r[c]) for c in cols) + " |")
    return "\n".join(lines)


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--results", type=Path, default=DEFAULT)
    results = p.parse_args().results
    _style()
    summary: dict[str, Any] = {}
    md = [
        f"# Local benchmark summary\n\n{SUBTITLE}\n\n"
        "Generated by `bench/plot.py` from the reports in this directory.\n"
    ]
    cols = [
        "label",
        "workers",
        "concurrency",
        "offered_per_s",
        "completed_per_s",
        "e2e_p50_ms",
        "e2e_p99_ms",
        "redis_main_thread",
        "worker_max",
        "loadgen",
        "vm_busy_cpus",
        "depth_min",
        "rejected",
        "blocked",
        "exactly_once",
    ]
    for suite, plotter in [
        ("scaling", plot_scaling),
        ("concurrency", plot_concurrency),
        ("latency", plot_latency),
    ]:
        reports = load(results, suite)
        if not reports:
            continue
        rows = [_row(r) for r in reports]
        summary[suite] = rows
        plotter(rows, results / f"{suite}.png")
        if suite == "scaling":
            plot_bottleneck(rows, results / "bottleneck.png")
        md += [f"## {suite}\n", _table(rows, cols), ""]
    bp = load(results, "backpressure")
    if bp:
        summary["backpressure"] = [_row(r) for r in bp]
        plot_backpressure(
            sorted(bp, key=lambda r: r["spec"]["backpressure"], reverse=True),
            results / "backpressure.png",
        )
        md += ["## backpressure\n", _table(summary["backpressure"], cols), ""]
    (results / "summary.json").write_text(json.dumps(summary, indent=1) + "\n")
    (results / "summary.md").write_text("\n".join(md) + "\n")
    print(f"wrote {results / 'summary.md'}")


if __name__ == "__main__":
    main()
