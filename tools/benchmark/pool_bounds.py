"""Aggregate a phase 8 pool-bounds run (spec docs/specs/2026-11-18-pool-bounds).

Stdlib only. Usage: python tools/benchmark/pool_bounds.py OUT DIR [DIR ...]
Each DIR is one job's target/benchmark (one repetition, every cell and arm on one runner). The
script merges their steps.jsonl and timeseries.jsonl into OUT, then writes summary.json and
results.md there. Comparisons against the baseline arm are paired within a job, so they hold the
runner's CPU fixed. The spec's baseline (the application as it is, write limiter on) runs as the
harness arm "current", because the phase 7 arm "baseline" means the limiter off.
"""

import json
import statistics
import sys
from collections import defaultdict
from pathlib import Path

BASE = "current"
ARMS = [BASE, "pool1s", "readlimit"]
WINDOW_SECONDS = 120


def read_jsonl(path):
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def med(values):
    values = [v for v in values if v is not None]
    return statistics.median(values) if values else None


def fmt(value, digits=0):
    if value is None:
        return "n/a"
    return f"{value:,.{digits}f}"


def pct(value):
    return "n/a" if value is None else f"{value:.2%}"


def share(row, key, scheduled_key):
    scheduled = row.get(scheduled_key) or 0
    return row.get(key, 0) / scheduled if scheduled else None


def kinds(row):
    """Per request kind: rate offered, served/s, p99, SLO, error shares by kind (503, other 5xx, timeout)."""
    out = {"writes": {
        "offered": row["offeredRps"], "served": row["createdPerSecond"], "p99Ms": row["p99Ms"],
        "slo": row["sloPass"], "share503": share(row, "unavailable503", "scheduled"),
        "share5xx": share(row, "otherServerErrors", "scheduled"), "shareTimeout": share(row, "timeouts", "scheduled")}}
    if row.get("offeredReadRps", 0) > 0:
        out["reads"] = {
            "offered": row["offeredReadRps"], "served": row["readOkPerSecond"], "p99Ms": row["readP99Ms"],
            "slo": row["readSloPass"], "share503": share(row, "read503", "readScheduled"),
            "share5xx": share(row, "readOtherServerErrors", "readScheduled"),
            "shareTimeout": share(row, "readTimeouts", "readScheduled")}
    return out


def main(out, dirs):
    out = Path(out)
    out.mkdir(parents=True, exist_ok=True)
    steps, series, runs, cpus = [], [], [], {}
    for d in map(Path, dirs):
        job_steps = read_jsonl(d / "steps.jsonl")
        steps += job_steps
        series += read_jsonl(d / "timeseries.jsonl")
        if (d / "run.json").exists():
            run = json.loads((d / "run.json").read_text(encoding="utf-8"))
            runs.append(run)
            for rep in {s["repetition"] for s in job_steps}:
                cpus[rep] = run.get("cpu")
    (out / "steps.jsonl").write_text("".join(json.dumps(s) + "\n" for s in steps), encoding="utf-8")
    (out / "timeseries.jsonl").write_text("".join(json.dumps(s) + "\n" for s in series), encoding="utf-8")

    by = defaultdict(dict)  # (name, arm) -> repetition -> {step index: row}
    for s in steps:
        by[(s["name"], s["arm"])].setdefault(s["repetition"], {})[s["step"]] = s
    repetitions = sorted({s["repetition"] for s in steps})
    names = sorted({name for name, _ in by})
    arms = [a for a in ARMS if any((n, a) in by for n in names)]

    def row(name, arm, rep, index):
        return by.get((name, arm), {}).get(rep, {}).get(index)

    summary = {"runs": runs, "cells": {}, "breakPoints": {}, "backlog": {}}
    lines = ["# Pool-bounds results", ""]
    lines.append(f"{len(repetitions)} jobs, one repetition each; every cell and arm of a repetition ran on the same "
                 "runner. CPU per repetition: " + ", ".join(f"rep {r}: {cpus.get(r, '?')}" for r in repetitions)
                 + f". The baseline arm is `{BASE}` (the application as it is: write limiter on, pool timeout 30 s).")
    lines.append("")

    # Per cell, arm and step: reads and writes side by side, medians over repetitions, and the
    # median of the per-job differences against the baseline arm.
    for name in names:
        lines += [f"## Cell `{name}`", "",
                  "Medians over repetitions. Served/s is created transfers for writes and 200 answers for reads. "
                  f"The vs {BASE} columns are the median of the per-job differences in served/s and p99.", "",
                  "| arm | step | kind | offered/s | within SLO | served/s | vs base | p99 ms | vs base | 503 | 5xx "
                  "| timeouts |", "|---|---|---|---|---|---|---|---|---|---|---|---|"]
        table = []
        for arm in arms:
            reps = by.get((name, arm), {})
            if not reps:
                continue
            n_steps = max(max(r) for r in reps.values()) + 1
            for index in range(n_steps):
                rows = [row(name, arm, rep, index) for rep in repetitions]
                rows = [r for r in rows if r]
                if not rows:
                    continue
                for kind in ("writes", "reads"):
                    per = [kinds(r).get(kind) for r in rows]
                    per = [p for p in per if p]
                    if not per:
                        continue
                    served_diffs, p99_diffs = [], []
                    for rep in repetitions:
                        a, b = row(name, arm, rep, index), row(name, BASE, rep, index)
                        ka, kb = (kinds(a).get(kind) if a else None), (kinds(b).get(kind) if b else None)
                        if ka and kb:
                            served_diffs.append(ka["served"] - kb["served"])
                            p99_diffs.append(ka["p99Ms"] - kb["p99Ms"])
                    entry = {"arm": arm, "step": index, "kind": kind, "runs": len(per),
                             "offered": per[0]["offered"], "sloPass": sum(1 for p in per if p["slo"]),
                             "served": med(p["served"] for p in per), "p99Ms": med(p["p99Ms"] for p in per),
                             "share503": med(p["share503"] for p in per), "share5xx": med(p["share5xx"] for p in per),
                             "shareTimeout": med(p["shareTimeout"] for p in per),
                             "servedVsBase": med(served_diffs) if arm != BASE else None,
                             "servedVsBaseRange": [min(served_diffs), max(served_diffs)]
                             if arm != BASE and served_diffs else None,
                             "p99VsBase": med(p99_diffs) if arm != BASE else None}
                    table.append(entry)
                    vs_served = "" if entry["servedVsBase"] is None else f"{entry['servedVsBase']:+,.0f}"
                    vs_p99 = "" if entry["p99VsBase"] is None else f"{entry['p99VsBase']:+,.0f}"
                    lines.append(f"| {arm} | {index} | {kind} | {fmt(entry['offered'])} | "
                                 f"{entry['sloPass']}/{entry['runs']} | {fmt(entry['served'])} | {vs_served} | "
                                 f"{fmt(entry['p99Ms'])} | {vs_p99} | {pct(entry['share503'])} | "
                                 f"{pct(entry['share5xx'])} | {pct(entry['shareTimeout'])} |")
        summary["cells"][name] = table
        lines.append("")

    # Where reads break each arm, and whether the read flood breaks the writes: the first step
    # of the ladder outside the SLO, per job. The last step of the reads cell is the recovery
    # probe and is reported apart.
    reads_cells = [n for n in names if any("offeredReadRps" in s and s["offeredReadRps"] > 0
                                           for reps in (by.get((n, a), {}) for a in arms)
                                           for rows in reps.values() for s in rows.values())]
    for name in reads_cells:
        lines += [f"## Break points in `{name}`", "",
                  "Per job: the read rate of the first ladder step whose reads (or writes) missed the SLO, and whether "
                  "the recovery probe (the last step) passed for both kinds. `none` means every ladder step passed.",
                  "", "| arm | rep | reads break at | writes break at | probe within SLO |", "|---|---|---|---|---|"]
        points = []
        for arm in arms:
            for rep in repetitions:
                rows = by.get((name, arm), {}).get(rep)
                if not rows:
                    continue
                ordered = [rows[i] for i in sorted(rows)]
                ladder, probe = ordered[:-1], ordered[-1]
                read_break = next((r["offeredReadRps"] for r in ladder if not r.get("readSloPass", True)), None)
                write_break = next((r["offeredReadRps"] for r in ladder if not r["sloPass"]), None)
                probe_ok = probe["sloPass"] and probe.get("readSloPass", True)
                points.append({"arm": arm, "repetition": rep, "readsBreakAt": read_break,
                               "writesBreakAt": write_break, "probePass": probe_ok})
                lines.append(f"| {arm} | {rep} | {fmt(read_break) if read_break else 'none'} | "
                             f"{fmt(write_break) if write_break else 'none'} | {probe_ok} |")
        summary["breakPoints"][name] = points
        lines.append("")

    # Backlog: the outbox and consumer per 2 minute window of each step, and the drain time.
    for name in [n for n in names if n not in reads_cells]:
        lines += [f"## Outbox and consumer in `{name}`", "",
                  "Per 2 minute window of each step (steps run back to back): the largest outbox oldest pending age, "
                  "outbox backlog and consumer lag seen in the window, medians over repetitions. Drain: seconds into "
                  "the last step until the outbox backlog first reads 0, and the settle time after the last step "
                  "(outbox empty and every transfer in the statement, capped at 2 minutes).", "",
                  "| arm | step | window | oldest pending age s | outbox pending | consumer lag |",
                  "|---|---|---|---|---|---|"]
        backlog = {}
        for arm in arms:
            cell_series = defaultdict(list)  # (rep, step) -> seconds
            for t in series:
                if t["arm"] == arm and t["cell"].startswith(f"{name}-"):
                    cell_series[(t["repetition"], t["step"])].append(t)
            if not cell_series:
                continue
            step_indexes = sorted({k[1] for k in cell_series})
            windows = []
            for index in step_indexes:
                n_windows = max(max(t["second"] for t in secs) for (r, i), secs in cell_series.items()
                                if i == index) // WINDOW_SECONDS + 1
                for w in range(n_windows):
                    def worst(key):
                        values = []
                        for (rep, i), secs in cell_series.items():
                            if i != index:
                                continue
                            v = [t.get(key) for t in secs if w * WINDOW_SECONDS <= t["second"] < (w + 1) * WINDOW_SECONDS
                                 and t.get(key) is not None]
                            if v:
                                values.append(max(v))
                        return med(values)
                    entry = {"step": index, "window": w + 1, "oldestPendingAgeSeconds": worst("outboxOldestAgeSeconds"),
                             "outboxPending": worst("outboxPending"), "consumerLag": worst("consumerLag")}
                    windows.append(entry)
                    lines.append(f"| {arm} | {index} | {w * 2}-{w * 2 + 2} min | "
                                 f"{fmt(entry['oldestPendingAgeSeconds'], 1)} | {fmt(entry['outboxPending'])} | "
                                 f"{fmt(entry['consumerLag'])} |")
            last = step_indexes[-1]
            drains = []
            for rep in repetitions:
                secs = sorted(cell_series.get((rep, last), []), key=lambda t: t["second"])
                first_empty = next((t["second"] for t in secs if t.get("outboxPending") == 0), None)
                last_row = row(name, arm, rep, last)
                drains.append({"repetition": rep, "outboxEmptyAtSecond": first_empty,
                               "settled": last_row.get("settled") if last_row else None,
                               "settleSeconds": last_row.get("settleSeconds") if last_row else None})
            backlog[arm] = {"windows": windows, "drain": drains}
        summary["backlog"][name] = backlog
        lines += ["", "| arm | rep | outbox empty at second (last step) | settled after the last step | settle s |",
                  "|---|---|---|---|---|"]
        for arm, b in backlog.items():
            for d in b["drain"]:
                lines.append(f"| {arm} | {d['repetition']} | "
                             f"{d['outboxEmptyAtSecond'] if d['outboxEmptyAtSecond'] is not None else 'never'} | "
                             f"{d['settled']} | {fmt(d['settleSeconds'], 1)} |")
        lines.append("")

    broken = [(s["cell"], s["repetition"], s["step"]) for s in steps
              if not s["ledgerOk"] or s.get("client4xx", 0) or s.get("read4xx", 0)]
    expected = set(range(1, max(repetitions, default=0) + 1))
    missing = sorted(expected - set(repetitions))
    failures = [f for d in map(Path, dirs) for f in read_jsonl(d / "failures.jsonl")]
    summary["missingRepetitions"], summary["failedRuns"] = missing, failures
    if missing or failures:
        lines.append(f"Missing repetitions: {missing or 'none'}. Runs that failed to complete: "
                     + (", ".join(f"{f['cell']} rep {f['repetition']} ({f['error']})" for f in failures) or "none")
                     + ".")
        lines.append("")
    lines.append("Ledger checks and 4xx: " + ("every step of every arm passed, no 4xx." if not broken
                                              else f"FAILED in {len(broken)} steps: {broken}"))
    lines.append("")
    (out / "summary.json").write_text(json.dumps(summary, indent=2, default=str) + "\n", encoding="utf-8")
    (out / "results.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2:])
