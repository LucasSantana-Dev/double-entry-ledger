---
status: in-progress
date: 2026-11-18
phase: 8
tags: [benchmark, overload, admission-control, connection-pool, outbox]
---

# Pool bounds: what still waits 30 s for a connection, and whether it matters

## Goal

ADR 0033 put a limiter in front of `POST /transfers` and `POST /deposits`. Three users of the same 10 connection pool stay outside it and still wait up to Hikari's default 30 s for a connection:

1. **Reads.** `GET /accounts/{id}` has no admission bound. A read flood past capacity has the same shape as the phase 6 collapse (virtual threads queueing for 30 s while the client gives up at 5 s), and those reads compete with the writes the limiter admitted.
2. **The outbox relay.** Every benchmark so far ran without a Kafka broker, so the relay never published under load; how far it falls behind at write capacity is unmeasured.
3. **The statement consumer.** Same: never measured under load.

ADR 0033 left a pool timeout "for when a run shows them queueing". This phase is that run. It answers, from paired within-job comparisons:

- Does a read flood break reads, writes, or both, and at what read rate?
- How far behind do the relay and the consumer fall at write capacity, and do they catch up?
- Which bound, if any, keeps reads and writes within the SLO past capacity: a 1 s pool timeout, or a read limiter?

The owner chooses from the results (ADR 0034).

## Scope

1. **Product changes, all off by default except the error mapping.**
   - **Pool timeout answers 503.** A request that fails to get a connection within the pool timeout (`CannotCreateTransactionException` or `CannotGetJdbcConnectionException` caused by Hikari's `SQLTransientConnectionException`) answers `503` with `Retry-After: 1` and an RFC 9457 problem (`/problems/overloaded`, the limiter's type), not a 500. With the default 30 s timeout no client waits that long, so this only changes what the `pool1s` arm answers.
   - **Read limiter.** `AdmissionFilter` gains a second, separate semaphore for `GET /accounts/{id}`: `ledger.admission.reads.max-concurrent` (0 = off, the default) and `ledger.admission.reads.max-wait`. Same 503 answer and a `kind` tag (`write`, `read`) on `ledger.admission.rejected`. The write semaphore and its defaults do not change.
2. **Harness.**
   - **Real Kafka.** The benchmark starts a Kafka container next to PostgreSQL and points the application at it, so the relay publishes and the statement consumer runs. The outbox gauges refresh every second in the benchmark (`ledger.outbox.metrics.refresh-ms=1000`).
   - **Mixed load.** The open-model generator (ADR 0032) drives two independent schedules in one step: transfers at a fixed rate and `GET /accounts/{id}` of a random funded account at the step's read rate. Latency, outcome and SLO are recorded per request kind.
   - **Per second samples**, added to `timeseries.jsonl`: `ledger.outbox.pending`, `ledger.outbox.oldest.pending.age`, the statement consumer's lag (`kafka.consumer.fetch.manager.records.lag.max`), and the existing pool series.
   - **After every step**, besides the ledger checks: the time until the outbox is empty and the statement has every transfer (reconciliation clean), capped at 2 minutes.
3. **Arms.**
   - `baseline`: the application as it is (write limiter on, 16 slots; pool timeout 30 s).
   - `pool1s`: `spring.datasource.hikari.connection-timeout=1000`.
   - `readlimit`: read limiter with 16 slots and a 100 ms wait.
4. **Cells.**
   - `reads`, 0% hot: transfers at 800 req/s (below the lowest phase 7 capacity, 1,200) and a read ladder whose rates the calibration run sets (draft: 2,000, 4,000, 8,000, 16,000), then a recovery probe at the first read rate. Steps of 2 minutes, no early stop.
   - `backlog`, 0% hot: transfers at 1,100 req/s and no reads for one continuous 10 minute step, read in 2 minute windows, then 2 minutes at 400 req/s to see whether the backlog drains.
5. **Design as in phase 7.** One calibration run (1 job) to place the read ladder, then 5 jobs in parallel, each running every cell and arm once in shuffled order on one runner, and an aggregate job. Warm-up, client timeout (5 s), SLO (p99 at most 500 ms and at most 1% errors, per request kind) and ledger checks as in ADR 0030.
6. **Aggregator** (stdlib Python, `tools/benchmark/pool_bounds.py`): per cell, arm and step, reads and writes side by side (rate offered, served/s, p99, error share by kind: 503, 5xx, timeout); per arm, the paired difference against baseline within each job; for `backlog`, outbox oldest pending age and consumer lag per 2 minute window and the drain time.
7. **Results** under `docs/benchmarks/<date>-pool-bounds/`, a section in `docs/results.md`, the README line updated, ADR 0034 written from the results with the owner's choice.

Out of scope: pool size tuning, separate pools per workload, adaptive limiters, statement reads (`GET` of statement lines), 90% hot.

## Acceptance criteria

1. With `ledger.admission.reads.max-concurrent` at 0 and the default pool timeout, every phase 1 to 7 test passes unchanged.
2. A test shows a pool timeout answers `503` with `Retry-After` and the `overloaded` problem type.
3. A test shows the read limiter answers `503` once its slots are taken, admits reads again once a slot frees, and never blocks writes (separate semaphore).
4. The relay and the consumer keep their behavior on a pool timeout: the relay logs and retries on the next tick, the consumer retries without limit and dead-letters nothing (existing tests, plus one with a 1 s pool timeout and the pool exhausted).
5. The benchmark runs with a real broker; `timeseries.jsonl` carries the outbox and consumer series; every step of every arm passes the ledger checks and has no 4xx answers.
6. The results state, from paired within-job comparisons, where reads break each arm, whether a read flood breaks writes, how far the relay and consumer fall behind at 1,100 req/s and whether they drain.

## Decisions

| Question | Decision | Record |
|---|---|---|
| Approach: measure before adding a bound | Measure first, chosen by the owner on 2026-11-17 | this spec |
| Bound for reads and background work (none, pool timeout, read limiter, both) and its settings | open: owner's choice from the results | ADR 0034 |

The arms, cells and rates above are the experiment's design, not a product decision.

## Done when

The aggregated results are committed, `docs/results.md` and the README carry what they show, ADR 0034 records the owner's choice, and the chosen bound is on by default if the owner picks one.
