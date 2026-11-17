# Breaking-point results

Run `44027992474` started 2026-11-17T06:00:31.766874151Z on AMD EPYC 7763 64-Core Processor (4 logical CPUs), Linux 7.0.0-1019-azure, Java 25.0.5+8-LTS. Strategy conditional-update, transfers only, 5 repetitions in shuffled order. Steps of PT2M after a PT30S warm-up at 250 req/s; client timeout PT5S; the ladder stops after 2 steps in a row outside the SLO (0: never).

SLO (ADR 0030): p99 <= 500 ms over every scheduled request, measured from its scheduled send time, and errors (5xx, timeouts, connection errors, unsent) <= 1% of scheduled requests. The load generator, the application and PostgreSQL share the same machine.

## Capacity

| cell | hot | capacity req/s | per repetition | SLO missed in | hard failure | ledger checks |
|---|---|---|---|---|---|---|
| conditional-update-hot00 | 0% | 0 | 0, 0, 0, 0, 0 | 5/5 | none | all passed |

Capacity is the median over repetitions, with its 95% bootstrap interval in brackets when the repetitions differ. Step granularity bounds it: a capacity of R means between R and the next step.

## Steps: conditional-update-hot00

| offered req/s | runs | within SLO | created/s | p50 ms | p99 ms | p99.9 ms | created p99 ms | errors | timeouts | 503 | heap MB | app CPU | generator CPU | send lag p99 ms | drain s |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1,500 | 5 | 0/5 | 80 | 5,000 | 5,000 | 5,000 | 4,787 | 94.69% | 131,434 | 0 | 1,015 | 0.56 | 0.13 | 94 | 6.3 |
| 1,750 | 5 | 0/5 | 110 | 5,000 | 5,000 | 5,000 | 4,630 | 93.73% | 148,990 | 0 | 1,008 | 0.46 | 0.18 | 95 | 6.1 |

Each step value is the median over the repetitions that reached that step. Latencies equal to the client timeout (PT5S) mean at or beyond it. CPU columns are the mean share of all logical CPUs.
