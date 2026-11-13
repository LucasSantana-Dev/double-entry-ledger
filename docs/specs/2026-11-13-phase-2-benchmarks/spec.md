---
status: in-progress
date: 2026-11-13
phase: 2
tags: [benchmark, concurrency, postgres, virtual-threads, jmeter]
---

# Phase 2: concurrency strategies and thread modes under load

## Goal

Put numbers on the cost that ADR 0007 accepted: compare four ways of keeping balances correct under concurrent transfers, and check whether virtual threads change throughput or latency when the database pool is the bottleneck. Every strategy keeps the phase 1 guarantees: the invariant test is the contract.

## Scope

1. Four concurrency strategies behind one interface, selected by the property `ledger.concurrency.strategy` (ADR 0010):
   - `pessimistic`: `SELECT ... FOR UPDATE` on both accounts in id order (phase 1 behavior, the default).
   - `optimistic`: JPA `@Version` on `accounts`, retry on version conflict.
   - `serializable`: transaction at `SERIALIZABLE` isolation, retry on SQLSTATE `40001`.
   - `conditional-update`: one `UPDATE ... SET balance_cents = balance_cents - x WHERE id = ? AND balance_cents >= x` for the source and one credit `UPDATE` for the destination, in id order, row count checked; no prior read of the balance.
2. Bounded retry for conflict aborts: up to 5 attempts, exponential backoff with jitter (about 1, 2, 4, 8 ms). When attempts run out, the API answers 503 (see below).
3. `ConcurrentTransfersInvariantTest` runs once per strategy with the same assertions. Its client treats 503 like 409: wait, retry with the same key.
4. Load plan with JMeter through jmeter-java-dsl (ADR 0011), in a Maven profile `benchmark` that `./mvnw verify` does not run.
5. Scenario matrix (ADR 0012), 1,000 accounts, HikariCP pool 10, 30 s warm-up, 60 s measured per cell:
   - Strategy grid: 4 strategies x hot account share 0%, 30%, 90% at 256 users, virtual threads. 12 cells.
   - Thread grid: `pessimistic` x 0%, 30%, 90% x 64 and 1,024 users x `spring.threads.virtual.enabled` true and false. 12 cells.
6. Metrics per cell (ADR 0012): created transfers per second; latency p50, p95, p99, max; server retries, 503 and 409 counts; deadlocks and serialization failures from `pg_stat_database` deltas; peak live threads, used heap, process CPU from Actuator.
7. A manual GitHub Actions workflow (ADR 0013) runs the 24 cells 5 times in shuffled order in one job and publishes a results file with median and 95% bootstrap confidence interval per cell, plus run id, date and CPU model.
8. README results section with the numbers from one published run and the stated limits.

Out of scope: choosing a production strategy for later phases (a follow-up ADR may, from the results), HikariCP pool sweeps, open-model load, other databases, horizontal scaling.

## API change

| Status | Type | When |
|---|---|---|
| 503 | `concurrency-conflict` | the strategy aborted on conflict 5 times in a row; header `Retry-After: 1`. Retry with the same key. |

Everything else in the phase 1 API is unchanged.

## Invariants

The seven invariants of the phase 1 spec, unchanged, for every strategy.

## Decisions

| ADR | Decision |
|---|---|
| 0010 | The four strategies of ADR 0007; bounded retry, then 503 |
| 0011 | JMeter through jmeter-java-dsl (falls back to Gatling if it does not run on Java 25) |
| 0012 | Two grids, 24 cells; throughput, latency, conflicts, JVM resources |
| 0013 | Published numbers from GitHub Actions, 5 shuffled repetitions in one job |

## Done when

- `./mvnw verify` passes with the invariant test green for all four strategies.
- `./mvnw -Pbenchmark ...` runs one cell locally and writes its metrics.
- The benchmark workflow completed one full run, and its results file is committed.
- README reports the results and their limits; ADR 0007 points to the results.
