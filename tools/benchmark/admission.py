"""Aggregate a phase 7 admission-control run (spec docs/specs/2026-11-17-admission-control).

Stdlib only. Usage: python tools/benchmark/admission.py OUT DIR [DIR ...]
Each DIR is one job's target/benchmark (one repetition, every cell and arm on one runner). The
script merges their steps.jsonl and timeseries.jsonl into OUT, then writes summary.json and
results.md there. Comparisons are paired within a job, so they hold the runner's CPU fixed.
"""

import json
import re
import statistics
import sys
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

ARMS = ["baseline", "hikari", "limiter"]
SLOW_MS = 500
NEAR_SECONDS = 5
PG_TIME = re.compile(r"^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3}) UTC")
ELAPSED = re.compile(r"elapsed: (\d+(?:\.\d+)?) s")
GC_LINE = re.compile(r"^\[(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3})\+0000\].*Pause.* (\d+\.\d+)ms$")


def read_jsonl(path):
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]


def parse_instant(text):
    return datetime.fromisoformat(text.replace("Z", "+00:00"))


def events(log_dir, prefix):
    """Intervals (start, end) of checkpoints, autovacuum runs and GC pauses of 50 ms or more.

    A checkpoint runs from its "starting" line to its "complete" line. An autovacuum line is
    logged when the run ends; its "elapsed" figure comes on a following line. A GC line is logged
    at the end of the pause and carries its length.
    """
    found = {"checkpoint": [], "autovacuum": [], "gc": []}
    pg = log_dir / f"{prefix}-postgres.log"
    if pg.exists():
        lines = pg.read_text(encoding="utf-8", errors="replace").splitlines()
        checkpoint_start = None
        for i, line in enumerate(lines):
            m = PG_TIME.match(line)
            if not m:
                continue
            at = datetime.strptime(m.group(1), "%Y-%m-%d %H:%M:%S.%f").replace(tzinfo=timezone.utc)
            if "checkpoint starting" in line:
                checkpoint_start = at
            elif "checkpoint complete" in line:
                found["checkpoint"].append((checkpoint_start or at, at))
                checkpoint_start = None
            elif "LOG:  automatic " in line and (" vacuum of " in line or " analyze of " in line):
                elapsed = 0.0
                for follow in lines[i + 1:i + 12]:
                    e = ELAPSED.search(follow)
                    if e:
                        elapsed = float(e.group(1))
                        break
                    if PG_TIME.match(follow):
                        break
                found["autovacuum"].append((at - timedelta(seconds=elapsed), at))
        if checkpoint_start is not None:  # still running when the log was taken
            found["checkpoint"].append((checkpoint_start, checkpoint_start + timedelta(minutes=10)))
    gc = log_dir / f"{prefix}-gc.log"
    if gc.exists():
        for line in gc.read_text(encoding="utf-8", errors="replace").splitlines():
            m = GC_LINE.match(line)
            if m and float(m.group(2)) >= 50:
                end = datetime.strptime(m.group(1), "%Y-%m-%dT%H:%M:%S.%f").replace(tzinfo=timezone.utc)
                found["gc"].append((end - timedelta(milliseconds=float(m.group(2))), end))
    return found


def near(at, intervals):
    """True when the second [at, at + 1 s] is within NEAR_SECONDS of any interval."""
    low, high = at - timedelta(seconds=NEAR_SECONDS), at + timedelta(seconds=1 + NEAR_SECONDS)
    return any(start <= high and end >= low for start, end in intervals)


def med(values):
    values = [v for v in values if v is not None]
    return statistics.median(values) if values else None


def fmt(value, digits=0):
    if value is None:
        return "n/a"
    return f"{value:,.{digits}f}"


def main(out, dirs):
    out = Path(out)
    out.mkdir(parents=True, exist_ok=True)
    steps, series, runs = [], [], []
    log_dirs = {}
    for d in map(Path, dirs):
        job_steps = read_jsonl(d / "steps.jsonl")
        steps += job_steps
        series += read_jsonl(d / "timeseries.jsonl")
        if (d / "run.json").exists():
            run = json.loads((d / "run.json").read_text(encoding="utf-8"))
            runs.append(run)
            for rep in {s["repetition"] for s in job_steps}:
                log_dirs[rep] = (d / "logs", run.get("cpu"))
    (out / "steps.jsonl").write_text("".join(json.dumps(s) + "\n" for s in steps), encoding="utf-8")
    (out / "timeseries.jsonl").write_text("".join(json.dumps(s) + "\n" for s in series), encoding="utf-8")

    by = defaultdict(dict)  # (name, arm) -> repetition -> [steps by index]
    for s in steps:
        by[(s["name"], s["arm"])].setdefault(s["repetition"], []).append(s)
    for reps in by.values():
        for rows in reps.values():
            rows.sort(key=lambda s: s["step"])
    repetitions = sorted({s["repetition"] for s in steps})

    def step(name, arm, rep, index):
        return next((s for s in by.get((name, arm), {}).get(rep, []) if s["step"] == index), None)

    summary = {"runs": runs, "cells": {}, "pathEffect": [], "arms": {}, "hold": {}}
    lines = ["# Admission-control results", ""]
    lines.append(f"{len(repetitions)} jobs, one repetition each; every cell and arm of a repetition ran on the same "
                 "runner. CPU per repetition: " + ", ".join(f"rep {r}: {log_dirs.get(r, (None, '?'))[1]}"
                                                            for r in repetitions) + ".")
    lines.append("")

    # Step tables per cell and arm.
    for (name, arm), reps in sorted(by.items()):
        n_steps = max(len(rows) for rows in reps.values())
        table = []
        for index in range(n_steps):
            rows = [rows[index] for rows in reps.values() if index < len(rows)]
            table.append({
                "step": index, "offeredRps": rows[0]["offeredRps"], "runs": len(rows),
                "sloPass": sum(1 for r in rows if r["sloPass"]),
                "createdPerSecond": med(r["createdPerSecond"] for r in rows),
                "p99Ms": med(r["p99Ms"] for r in rows),
                "errorShare": med(r["errorShare"] for r in rows),
                "unavailable503": med(r["unavailable503"] for r in rows),
                "timeouts": med(r["timeouts"] for r in rows),
                "peakHeapMb": med(r["peakHeapMb"] for r in rows),
                "ledgerOk": all(r["ledgerOk"] for r in rows),
                "client4xx": sum(r["client4xx"] for r in rows),
            })
        summary["cells"][f"{name}-{arm}"] = table

    # Path effect: climb at 1,500 against jump at 1,500, same arm, same job.
    lines += ["## Path effect (same runner)", "",
              "| arm | rep | climb at 1,500 within SLO | jump to 1,500 within SLO | climb created/s | jump created/s |",
              "|---|---|---|---|---|---|"]
    for arm in ARMS:
        for rep in repetitions:
            climb = next((s for s in by.get(("climb", arm), {}).get(rep, []) if s["offeredRps"] == 1500), None)
            jump = step("jump", arm, rep, 0)
            if not climb or not jump:
                continue
            summary["pathEffect"].append({"arm": arm, "repetition": rep, "climbPass": climb["sloPass"],
                                          "jumpPass": jump["sloPass"], "climbCreated": climb["createdPerSecond"],
                                          "jumpCreated": jump["createdPerSecond"]})
            lines.append(f"| {arm} | {rep} | {climb['sloPass']} | {jump['sloPass']} | "
                         f"{fmt(climb['createdPerSecond'])} | {fmt(jump['createdPerSecond'])} |")
    lines.append("")

    # Arms against baseline on the jump and the probe, paired by job.
    lines += ["## Arms under the jump to 1,500 req/s", "",
              "Medians over repetitions; the difference column is the median of the per-job differences against "
              "baseline.", "",
              "| arm | step | within SLO | created/s | vs baseline | p99 ms | errors | 503 | timeouts | heap MB |",
              "|---|---|---|---|---|---|---|---|---|---|"]
    labels = {0: "1,500 (first 2 min)", 1: "1,500 (next 2 min)", 2: "probe at 1,000"}
    for arm in ARMS:
        for index, label in labels.items():
            rows = [step("jump", arm, rep, index) for rep in repetitions]
            rows = [r for r in rows if r]
            if not rows:
                continue
            diffs = []
            for rep in repetitions:
                a, b = step("jump", arm, rep, index), step("jump", "baseline", rep, index)
                if a and b:
                    diffs.append(a["createdPerSecond"] - b["createdPerSecond"])
            entry = {"step": index, "runs": len(rows), "sloPass": sum(1 for r in rows if r["sloPass"]),
                     "createdPerSecond": med(r["createdPerSecond"] for r in rows),
                     "createdVsBaseline": med(diffs) if arm != "baseline" else None,
                     "p99Ms": med(r["p99Ms"] for r in rows), "errorShare": med(r["errorShare"] for r in rows),
                     "unavailable503": med(r["unavailable503"] for r in rows),
                     "timeouts": med(r["timeouts"] for r in rows), "peakHeapMb": med(r["peakHeapMb"] for r in rows)}
            summary["arms"].setdefault(arm, []).append(entry)
            delta = entry["createdVsBaseline"]
            vs = "" if arm == "baseline" or delta is None else f"{delta:+,.0f}"
            lines.append(f"| {arm} | {label} | {entry['sloPass']}/{entry['runs']} | {fmt(entry['createdPerSecond'])} | "
                         f"{vs} | {fmt(entry['p99Ms'])} | {entry['errorShare']:.2%} | {fmt(entry['unavailable503'])} | "
                         f"{fmt(entry['timeouts'])} | {fmt(entry['peakHeapMb'])} |")
    lines.append("")

    # Hold at 90% hot: one continuous step, read per second from the time series.
    start_of = {(s["cell"], s["repetition"], s["step"]): parse_instant(s["startedAt"]) for s in steps if "startedAt" in s}
    lines += ["## Hold at 650 req/s, 90% hot, 10 minutes without a break", "",
              "Per 2 minute window of the one continuous step: created/s, and seconds whose p99 passed "
              f"{SLOW_MS} ms (median over repetitions). Requests are counted in the second they were scheduled.", "",
              "| arm | step within SLO | window | created/s | slow seconds |", "|---|---|---|---|---|"]
    hold = {}
    for arm in ARMS:
        cell = f"hold-hot90-{arm}"
        per_rep = {}
        for rep in repetitions:
            seconds = sorted((t for t in series if t["cell"] == cell and t["repetition"] == rep),
                             key=lambda t: (t["step"], t["second"]))
            if seconds:
                per_rep[rep] = seconds
        if not per_rep:
            continue
        passed = sum(1 for rep in per_rep if (step("hold", arm, rep, 0) or {}).get("sloPass"))
        windows = []
        for w in range(5):
            created = [sum(t["created"] for t in secs if w * 120 <= t["second"] < (w + 1) * 120) / 120
                       for secs in per_rep.values()]
            slow = [sum(1 for t in secs if w * 120 <= t["second"] < (w + 1) * 120 and t["p99Ms"] > SLOW_MS)
                    for secs in per_rep.values()]
            windows.append({"window": w + 1, "createdPerSecond": med(created), "slowSeconds": med(slow)})
            lines.append(f"| {arm} | {passed}/{len(per_rep)} | {w * 2}-{w * 2 + 2} min | "
                         f"{fmt(med(created))} | {fmt(med(slow))} |")

        # Slow-second onsets against PostgreSQL and GC events, with the chance rate: the share of
        # all held seconds that fall near an event of that kind.
        onsets, all_seconds = [], 0
        near_any = {"checkpoint": 0, "autovacuum": 0, "gc": 0}
        missing_pool = 0
        for rep, secs in per_rep.items():
            found = events(log_dirs[rep][0], f"{cell}-rep{rep}") if rep in log_dirs else {}
            previous_slow = False
            for t in secs:
                start = start_of.get((cell, rep, t["step"]))
                if start is None:
                    continue
                at = start + timedelta(seconds=t["second"])
                all_seconds += 1
                missing_pool += 1 if t.get("poolPending") is None else 0
                flags = {kind: near(at, intervals) for kind, intervals in found.items()}
                for kind, hit in flags.items():
                    near_any[kind] += 1 if hit else 0
                slow = t["p99Ms"] > SLOW_MS
                if slow and not previous_slow:
                    onsets.append({"repetition": rep, "second": t["second"],
                                   "poolPending": t.get("poolPending"), **flags})
                previous_slow = slow
        chance = {kind: (n / all_seconds if all_seconds else 0) for kind, n in near_any.items()}
        hold[arm] = {"stepWithinSlo": passed, "repetitions": len(per_rep), "windows": windows, "onsets": onsets,
                     "chance": chance, "heldSeconds": all_seconds, "secondsWithoutPoolSample": missing_pool}
    summary["hold"] = hold
    lines += ["", f"Slow-second onsets (a second whose p99 passes {SLOW_MS} ms after one that did not) near a "
              f"PostgreSQL checkpoint, an autovacuum run or a GC pause of 50 ms or more (within {NEAR_SECONDS} s of "
              "the event's interval), against chance: the share of all held seconds near such an event. An "
              "observed share well above chance links the stalls to that event; near chance, it does not.", "",
              "| arm | onsets | near a checkpoint (chance) | near autovacuum (chance) | near a GC pause (chance) | "
              "onsets near none | seconds without a pool sample |", "|---|---|---|---|---|---|---|"]
    for arm, h in hold.items():
        o = h["onsets"]

        def share(kind):
            hits = sum(1 for x in o if x.get(kind))
            observed = f"{hits}/{len(o)}" if o else "0/0"
            return f"{observed} ({h['chance'][kind]:.0%})"

        none = sum(1 for x in o if not any(x.get(k) for k in ("checkpoint", "autovacuum", "gc")))
        lines.append(f"| {arm} | {len(o)} | {share('checkpoint')} | {share('autovacuum')} | {share('gc')} | "
                     f"{none} | {h['secondsWithoutPoolSample']}/{h['heldSeconds']} |")
    lines.append("")

    broken = [(k, s) for k, t in summary["cells"].items() for s in t if not s["ledgerOk"] or s["client4xx"]]
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
    lines.append("Per-step tables for every cell and arm are in summary.json (`cells`).")
    (out / "summary.json").write_text(json.dumps(summary, indent=2, default=str) + "\n", encoding="utf-8")
    (out / "results.md").write_text("\n".join(lines) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2:])
