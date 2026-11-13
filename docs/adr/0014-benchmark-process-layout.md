# 0014. Each benchmark cell starts the application as a child JVM on a fresh PostgreSQL

- Status: accepted
- Date: 2026-11-13

## Context

The JMeter spike (ADR 0011) ran load generator and application in one JVM. The thread grid of ADR 0012 reports the application's live threads, heap and CPU, which 1,024 JMeter threads in the same process would pollute. The concurrency strategy and the thread mode are startup properties, so the application restarts for every cell whatever the layout. Each cell also writes thousands of transfers, and the rows and dead tuples they leave behind would slow the cells that follow.

## Options

Process layout:

1. The benchmark test (the JMeter JVM) starts PostgreSQL with Testcontainers and launches `java -jar` on the packaged application as a child process with the cell's properties; it reads the application's metrics from `/actuator/metrics` and stops the process at the end of the cell.
2. Docker compose with the application in a container, restarted per cell. Anticipates the phase 4 Dockerfile and allows CPU limits, but puts Docker networking on every request and adds a Dockerfile before the phase that specifies it.
3. One JVM, application threads counted by name. Simplest, but heap and CPU cannot be split between application and JMeter, and both share one GC.

Database state between cells:

1. A new PostgreSQL container per cell, migrated by Flyway and seeded with 1,000 accounts. A few seconds per cell, about 6 to 10 minutes over a full run of 120 cells.
2. One container per run, `TRUNCATE`, reseed and `VACUUM ANALYZE` between cells. Faster, but page cache and statistics carry over from the previous cell.

## Decision

Option 1 for both: a child JVM per cell, on a fresh PostgreSQL container per cell.

## Consequences

- The benchmark runs after packaging: the `benchmark` profile runs the `*Benchmark` classes with Failsafe in `./mvnw -Pbenchmark verify` and skips the unit tests, which CI already runs.
- The application's JVM options and pool sizes are pinned on its command line (heap, HikariCP pool 10, Tomcat 200 threads), so a cell's configuration is visible in one place and in the results.
- Each cell ends with a cheap ledger check (balances sum to zero, no customer below zero, every balance equals its entries). A benchmark never reports numbers from a broken ledger.
- PostgreSQL's `pg_stat_database` counts deadlocks but not serialization failures. Conflict aborts of every kind are counted by the application's retry counters (ADR 0010); deadlocks are also reported from `pg_stat_database`.
