---
status: done
date: 2026-11-17
phase: 7
tags: [benchmark, capacity, overload, admission-control, metastability]
---

# Admission control: why the ledger collapses past its capacity, and whether bounding admission fixes it

## Goal

The breaking-point runs (phase 6, `docs/results.md#breaking-point`) left four claims untested:

1. At 0% hot, 1,500 req/s held when the rate climbed to it in 100 req/s steps and collapsed when it came as a jump, but climb and jump ran on different CPU models.
2. The 0% hot collapse looks like congestion collapse with no admission control (about 8,180 requests waiting up to 30 s for 10 database connections while the client gives up at 5 s), but no run removed that mechanism.
3. The 90% hot failures are not monotonic (break at 650 req/s, hold at 700) and have no diagnosed cause: a load limit and a periodic stall (checkpoint, autovacuum, GC) both fit, because rate, elapsed time and database size grow together along the ladder.
4. Bounding admission should turn collapse into fast 503s and more useful work under overload, without raising the capacity.

This phase settles them with one experiment in which every comparison runs on the same runner, and gives the owner the data to choose an admission mechanism (ADR 0033).

## Scope

1. **Instrumentation, no behavior change.** Per second of every step: database pool waiters (`hikaricp.connections.pending`), mean pool acquire time, requests in progress, and the outcome counts of the requests scheduled in that second, to `timeseries.jsonl`. PostgreSQL logs checkpoints, autovacuum runs and lock waits; the application JVM writes a GC log; both are kept per cell with the application log. The CPU model is already in `run.json`.
2. **Two admission arms, both off by default.**
   - `hikari`: `spring.datasource.hikari.connection-timeout=1000` (the pool wait drops from 30 s to 1 s, below the 5 s client timeout). Configuration only.
   - `limiter`: a servlet filter in front of `POST /transfers` and `POST /deposits` that admits at most `ledger.admission.max-concurrent` requests at a time and answers `503` with `Retry-After: 1` and an RFC 9457 problem (`type` ending in `overloaded`) when no slot frees up within `ledger.admission.max-wait`. Off unless `max-concurrent` is above 0. The run uses 16 slots (the pool has 10 connections, one held by the outbox relay) and a 100 ms wait. A meter counts rejections.
   - `baseline`: the application as it is.
3. **Cells.** For each arm:
   - `climb`, 0% hot: 1,000 to 1,500 req/s in steps of 100, then a recovery probe at 1,000.
   - `jump`, 0% hot: 1,500 req/s straight after the warm-up, held for two steps, then a recovery probe at 1,000.
   - `hold`, 90% hot: 650 req/s for one continuous 10 minute step (no drain or ledger check in between), so elapsed time and database size grow while the rate stays fixed; the aggregator reads it in 2 minute windows from the per-second series.
   Steps of 2 minutes, no early stop, the phase 6 warm-up, timeout, SLO and ledger checks after every step.
4. **One repetition per job, every cell in the same job.** Each repetition is a GitHub Actions job running all 9 cells in shuffled order, so arms and paths are compared on one CPU (a block design); 5 jobs run in parallel and an aggregate job merges them.
5. **Aggregator** (stdlib Python): per cell and arm, the step table; per comparison, the paired difference within each job (climb against jump at 1,500; each arm against baseline on created/s, p99 and error share at 1,500 and in the probe); for `hold`, created/s and SLO per 2 minute step, with the timestamps of checkpoints, autovacuum runs and GC pauses from the logs next to the per-second pool queue.
6. **Results** under `docs/benchmarks/<date>-admission-control/`, the phase 6 section of `docs/results.md` updated, the README line updated, ADR 0033 written from the results with the owner's choice.

Out of scope: other strategies, tuning the pool size, adaptive limiters (gradient, AIMD), retry behavior in clients, the 30% hot share.

## Definitions

As in phase 6 (ADR 0030), plus:

- **Goodput**: created transfers per second of the step's nominal length.
- **Recovered**: the probe step after an overloaded step passes the SLO.
- **Path effect**: within one job, climb holds 1,500 req/s and jump does not, for the same arm.

## Acceptance criteria

1. With `ledger.admission.max-concurrent` at 0 the application behaves exactly as before, and the phase 1 to 5 tests pass unchanged with the limiter on (its default since ADR 0033).
2. A test shows the limiter answers `503` with `Retry-After` and the `overloaded` problem type once its slots are taken, and admits requests again once a slot frees.
3. The run writes `timeseries.jsonl` and per-cell PostgreSQL and GC logs; the aggregator reads them.
4. Every step of every arm passes the ledger checks and has no 4xx answers.
5. The results state, from paired within-job comparisons, whether the path effect exists on one CPU, whether each arm prevents the collapse and the recovery failure, and whether the 90% hot stalls line up with checkpoints, autovacuum or GC.

## Decisions

| Question | Decision | Record |
|---|---|---|
| Admission mechanism (none, pool timeout, limiter, both) and its settings | Limiter, 16 slots, 100 ms wait, on by default; chosen by the owner on 2026-11-17 | [ADR 0033](../../adr/0033-admission-control-limiter.md) |

The arms, cells and settings above are the experiment's design, not a product decision: they only exist to produce the data ADR 0033 needs.

## Done when

The aggregated results are committed, `docs/results.md` and the README carry what they show, ADR 0033 records the owner's choice, and the chosen mechanism is on by default if the owner picks one.
