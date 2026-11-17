"""Aggregate a breaking-point run (ADRs 0030 to 0032): capacity per cell and a per-step table.

Stdlib only. Usage: python tools/benchmark/breaking_point.py target/benchmark
Reads steps.jsonl (one line per step of every cell and repetition) and run.json from that
directory and writes summary.json and results.md next to them.

Capacity of one repetition: the offered rate of the last step that met the SLO before the first
step that missed it; 0 when the first step already missed it. When no step missed it, the
ladder was too short and the capacity is reported as "at least" the last rate.
"""

import json
import random
import statistics
import sys
from collections import defaultdict
from pathlib import Path

from aggregate import bootstrap_median_ci

STEP_METRICS = [
    "createdPerSecond", "p50Ms", "p99Ms", "p999Ms", "createdP99Ms", "errorShare",
    "timeouts", "unavailable503", "peakHeapMb", "meanAppCpu", "meanLoadGeneratorCpu",
    "sendLagP99Ms", "drainSeconds",
]
SEED = 20261116


def capacity(steps):
    """Capacity, whether the SLO was ever missed, and the hard failure if any, for one repetition."""
    last_pass = 0.0
    missed = False
    for step in steps:
        if step["sloPass"]:
            last_pass = step["offeredRps"]
        else:
            missed = True
            break
    hard = next((s for s in steps if not s["appAlive"]), None)
    return last_pass, missed, hard


def summarize(rows):
    rng = random.Random(SEED)
    by_run = defaultdict(list)
    for row in rows:
        by_run[(row["cell"], row["repetition"])].append(row)
    cells = defaultdict(lambda: {"capacities": [], "missed": 0, "hardFailures": [], "steps": defaultdict(list)})
    for (cell, repetition), steps in sorted(by_run.items()):
        steps.sort(key=lambda s: s["step"])
        cap, missed, hard = capacity(steps)
        entry = cells[cell]
        entry["hotShare"] = steps[0]["hotShare"]
        entry["strategy"] = steps[0]["strategy"]
        entry["capacities"].append(cap)
        entry["missed"] += 1 if missed else 0
        if hard:
            entry["hardFailures"].append({"repetition": repetition, "offeredRps": hard["offeredRps"],
                                          "exit": hard.get("exit", "exited")})
        for step in steps:
            entry["steps"][step["offeredRps"]].append(step)

    summary = {}
    for cell, entry in sorted(cells.items()):
        caps = entry["capacities"]
        low, high = bootstrap_median_ci(caps, rng)
        out = {
            "strategy": entry["strategy"],
            "hotShare": entry["hotShare"],
            "repetitions": len(caps),
            "capacity": {"median": statistics.median(caps), "ciLow": low, "ciHigh": high, "values": caps},
            "sloMissedInRepetitions": entry["missed"],
            "hardFailures": entry["hardFailures"],
            "ledgerAlwaysOk": all(s["ledgerOk"] for steps in entry["steps"].values() for s in steps),
            "steps": {},
        }
        for rate, steps in sorted(entry["steps"].items()):
            stats = {"runs": len(steps), "sloPass": sum(1 for s in steps if s["sloPass"])}
            for metric in STEP_METRICS:
                stats[metric] = statistics.median(s[metric] for s in steps)
            out["steps"][str(rate)] = stats
        summary[cell] = out
    return summary


def num(x, digits=0):
    return f"{x:,.{digits}f}"


def markdown(summary, run):
    lines = [
        "# Breaking-point results",
        "",
        f"Run `{run.get('githubRunId')}` started {run.get('startedAt')} on {run.get('cpu')} "
        f"({run.get('logicalCpus')} logical CPUs), {run.get('os')}, Java {run.get('java')}. "
        f"Strategy {run.get('strategy')}, transfers only, {run.get('repetitions')} repetitions in shuffled order. "
        f"Steps of {run.get('step')} after a {run.get('warmup')} warm-up at {num(run.get('warmupRate', 0))} req/s; "
        f"client timeout {run.get('timeout')}; the ladder stops after {run.get('stopAfterFailures')} steps in a row "
        "outside the SLO (0: never).",
        "",
        f"SLO (ADR 0030): p99 <= {num(run.get('sloP99Ms', 500))} ms over every scheduled request, measured from its "
        "scheduled send time, and errors (5xx, timeouts, connection errors, unsent) <= "
        f"{num(100 * run.get('sloErrorShare', 0.01))}% of scheduled requests. "
        "The load generator, the application and PostgreSQL share the same machine.",
        "",
        "## Capacity",
        "",
        "| cell | hot | capacity req/s | per repetition | SLO missed in | hard failure | ledger checks |",
        "|---|---|---|---|---|---|---|",
    ]
    for cell, s in summary.items():
        c = s["capacity"]
        if c["ciLow"] == c["ciHigh"]:
            cap = num(c["median"])
        else:
            cap = f"{num(c['median'])} [{num(c['ciLow'])}, {num(c['ciHigh'])}]"
        short = s["repetitions"] - s["sloMissedInRepetitions"]
        if short:
            cap = f">= {cap} (ladder too short in {short} repetitions)"
        hard = ", ".join(f"{num(h['offeredRps'])} req/s ({h['exit']}, rep {h['repetition']})"
                         for h in s["hardFailures"]) or "none"
        ledger = "all passed" if s["ledgerAlwaysOk"] else "FAILED"
        lines.append(f"| {cell} | {s['hotShare']:.0%} | {cap} | {', '.join(num(v) for v in c['values'])} | "
                     f"{s['sloMissedInRepetitions']}/{s['repetitions']} | {hard} | {ledger} |")
    lines += [
        "",
        "Capacity is the median over repetitions, with its 95% bootstrap interval in brackets when the repetitions "
        "differ. Step granularity bounds it: a capacity of R means between R and the next step.",
        "",
    ]
    for cell, s in summary.items():
        lines += [
            f"## Steps: {cell}",
            "",
            "| offered req/s | runs | within SLO | created/s | p50 ms | p99 ms | p99.9 ms | created p99 ms | errors | "
            "timeouts | 503 | heap MB | app CPU | generator CPU | send lag p99 ms | drain s |",
            "|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|",
        ]
        for rate, st in s["steps"].items():
            lines.append(
                f"| {num(float(rate))} | {st['runs']} | {st['sloPass']}/{st['runs']} | {num(st['createdPerSecond'])} | "
                f"{num(st['p50Ms'])} | {num(st['p99Ms'])} | {num(st['p999Ms'])} | {num(st['createdP99Ms'])} | "
                f"{st['errorShare']:.2%} | {num(st['timeouts'])} | {num(st['unavailable503'])} | "
                f"{num(st['peakHeapMb'])} | {st['meanAppCpu']:.2f} | {st['meanLoadGeneratorCpu']:.2f} | "
                f"{num(st['sendLagP99Ms'])} | {st['drainSeconds']:.1f} |")
        lines.append("")
    lines += [
        "Each step value is the median over the repetitions that reached that step. Latencies equal to the client "
        f"timeout ({run.get('timeout')}) mean at or beyond it. CPU columns are the mean share of all logical CPUs.",
        "",
    ]
    return "\n".join(lines)


def main(directory):
    directory = Path(directory)
    text = (directory / "steps.jsonl").read_text(encoding="utf-8")
    rows = [json.loads(line) for line in text.splitlines() if line.strip()]
    run = json.loads((directory / "run.json").read_text(encoding="utf-8"))
    summary = summarize(rows)
    (directory / "summary.json").write_text(json.dumps({"run": run, "cells": summary}, indent=2) + "\n",
                                            encoding="utf-8")
    (directory / "results.md").write_text(markdown(summary, run), encoding="utf-8")


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else "target/benchmark")
