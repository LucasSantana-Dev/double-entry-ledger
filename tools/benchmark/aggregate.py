"""Aggregate a benchmark run (ADR 0013): median and 95% bootstrap confidence interval per cell.

Stdlib only. Usage: python tools/benchmark/aggregate.py target/benchmark
Reads results.jsonl (one line per cell and repetition) and run.json from that directory and
writes summary.json and results.md next to them.

The interval is a percentile bootstrap of the median over the repetitions of a cell. With 5
repetitions it is coarse: it can only take values seen in the samples, so treat it as a range
of plausible medians, not a precise interval.
"""

import json
import random
import statistics
import sys
from collections import defaultdict
from pathlib import Path

METRICS = [
    "createdPerSecond", "p50Ms", "p95Ms", "p99Ms", "allP50Ms", "allP99Ms",
    "retries", "unavailable503", "deadlocks",
    "peakPlatformThreads", "peakHeapMb", "meanAppCpu", "meanLoadGeneratorCpu",
]
RESAMPLES = 10_000
SEED = 20261113


def bootstrap_median_ci(values, rng):
    if len(values) == 1:
        return values[0], values[0]
    medians = sorted(statistics.median(rng.choices(values, k=len(values))) for _ in range(RESAMPLES))
    return medians[int(0.025 * RESAMPLES)], medians[int(0.975 * RESAMPLES) - 1]


def summarize(rows):
    rng = random.Random(SEED)
    by_cell = defaultdict(list)
    for row in rows:
        by_cell[row["cell"]].append(row)
    summary = {}
    for cell, cell_rows in sorted(by_cell.items()):
        first = cell_rows[0]
        entry = {key: first[key] for key in ("grid", "strategy", "hotShare", "users", "threads")}
        entry["repetitions"] = len(cell_rows)
        for metric in METRICS:
            values = [row[metric] for row in cell_rows if row.get(metric, -1) != -1]
            if not values:
                entry[metric] = None
                continue
            low, high = bootstrap_median_ci(values, rng)
            entry[metric] = {"median": statistics.median(values), "ciLow": low, "ciHigh": high}
        summary[cell] = entry
    return summary


def fmt(stat, digits=0):
    if stat is None:
        return "n/a"
    def number(x):
        return f"{x:,.{digits}f}"
    if stat["ciLow"] == stat["ciHigh"]:
        return number(stat["median"])
    return f"{number(stat['median'])} [{number(stat['ciLow'])}, {number(stat['ciHigh'])}]"


def markdown(summary, run):
    lines = [
        "# Benchmark results",
        "",
        f"Run `{run.get('githubRunId')}` started {run.get('startedAt')} on {run.get('cpu')} "
        f"({run.get('logicalCpus')} logical CPUs), {run.get('os')}, Java {run.get('java')}. "
        f"Warm-up {run.get('warmup')}, measured {run.get('measure')}, {run.get('repetitions')} repetitions in shuffled order.",
        "",
        "Each value is the median over the repetitions, with its 95% bootstrap interval in brackets. "
        "Latency is for created transfers (201); `all p99` includes 503 answers (ADR 0015).",
        "",
    ]
    columns = [
        ("created/s", "createdPerSecond", 0), ("p50 ms", "p50Ms", 0), ("p99 ms", "p99Ms", 0),
        ("all p99 ms", "allP99Ms", 0), ("retries", "retries", 0), ("503", "unavailable503", 0),
        ("deadlocks", "deadlocks", 0), ("platform threads", "peakPlatformThreads", 0),
        ("heap MB", "peakHeapMb", 0), ("app CPU", "meanAppCpu", 2),
    ]
    for grid, title in (("strategy", "Strategy grid (256 users, virtual threads)"),
                        ("threads", "Thread grid (pessimistic strategy)")):
        cells = [(cell, entry) for cell, entry in summary.items() if entry["grid"] == grid]
        if not cells:
            continue
        lines += [f"## {title}", ""]
        lines.append("| strategy | hot | users | threads | " + " | ".join(c[0] for c in columns) + " |")
        lines.append("|---|---|---|---|" + "---|" * len(columns))
        cells.sort(key=lambda item: (item[1]["strategy"], item[1]["hotShare"], item[1]["users"], item[1]["threads"]))
        for _, entry in cells:
            row = [entry["strategy"], f"{entry['hotShare']:.0%}", str(entry["users"]), entry["threads"]]
            row += [fmt(entry[key], digits) for _, key, digits in columns]
            lines.append("| " + " | ".join(row) + " |")
        lines.append("")
    lines.append("App CPU is the mean share of all logical CPUs used by the application process.")
    return "\n".join(lines) + "\n"


def main():
    directory = Path(sys.argv[1] if len(sys.argv) > 1 else "target/benchmark")
    rows = [json.loads(line) for line in (directory / "results.jsonl").read_text(encoding="utf-8").splitlines() if line]
    run = json.loads((directory / "run.json").read_text(encoding="utf-8"))
    summary = summarize(rows)
    (directory / "summary.json").write_text(json.dumps({"run": run, "cells": summary}, indent=2) + "\n", encoding="utf-8")
    (directory / "results.md").write_text(markdown(summary, run), encoding="utf-8")
    print(f"{len(rows)} rows, {len(summary)} cells -> {directory / 'results.md'}")


if __name__ == "__main__":
    main()
