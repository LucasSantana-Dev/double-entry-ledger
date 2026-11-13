# 0012. Benchmark scenarios: two grids, 24 cells; throughput, latency, conflicts, JVM resources

- Status: accepted
- Date: 2026-11-13

## Context

Two questions share one benchmark: which concurrency strategy (ADR 0010) holds up under contention, and whether virtual threads change anything when the database pool is the bottleneck.

## Options

Scenario matrix (common base: 1,000 accounts, a share of transfers touching one hot account at 0%, 30% and 90%, HikariCP pool fixed at 10 in every cell, 30 s warm-up discarded, 60 s measured):

1. Two grids, 24 cells. Strategy grid: 4 strategies x 3 hot shares at 256 concurrent users on virtual threads. Thread grid: pessimistic strategy x 3 hot shares x 64 and 1,024 users x virtual and platform threads. About 36 minutes per repetition.
2. Full factorial, 48 cells, about 72 minutes per repetition. Shows interactions, but 5 repetitions hit the 6 hour limit of a GitHub Actions job.
3. Open model at fixed arrival rates in steps up to saturation. Avoids coordinated omission (Gil Tene), but needs a capacity run first and reports curves instead of one table.

Metrics: throughput and latency; conflicts; JVM resources; HikariCP connection wait.

## Evidence

- Pool size 10 is the HikariCP default and close to its "About Pool Sizing" formula, `cores x 2 + spindles`, which gives 9 on the 4 vCPU runner chosen in ADR 0013.
- Tomcat's default maximum is 200 request threads. 64 users stay below it and 1,024 go above it, so the thread grid tests the hypothesis that virtual threads only matter past that limit, because the 10 connections cap the work in flight either way.

## Decision

Option 1 for the matrix. Metrics per cell:

- Throughput (created transfers per second) and latency p50, p95, p99 and max, from the JMeter results.
- Conflicts: server-side retries, 503 and 409 responses, and deadlocks and serialization failures from `pg_stat_database` before and after the cell.
- JVM resources: peak live threads, used heap and process CPU, from Actuator and Micrometer.

HikariCP connection wait is not reported.

## Consequences

- The closed model can hide tail latency (coordinated omission). The results write-up states this limit.
- Interactions between strategy and thread mode are not measured. If the thread grid shows a difference, a follow-up can extend it to the other strategies.
- Without the pool wait metric, the claim "the pool is the bottleneck" stays an inference from flat throughput, and the write-up says so.
