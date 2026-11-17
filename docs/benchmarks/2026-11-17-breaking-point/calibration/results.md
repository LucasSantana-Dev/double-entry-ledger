# Breaking-point results

Run `44008070376` started 2026-11-17T03:09:13.310399436Z on AMD EPYC 9V74 80-Core Processor (4 logical CPUs), Linux 7.0.0-1019-azure, Java 25.0.5+8-LTS. Strategy conditional-update, transfers only, 1 repetitions in shuffled order. Steps of PT2M after a PT30S warm-up at 250 req/s; client timeout PT5S; the ladder stops after 0 steps in a row outside the SLO (0: never).

SLO (ADR 0030): p99 <= 500 ms over every scheduled request, measured from its scheduled send time, and errors (5xx, timeouts, connection errors, unsent) <= 1% of scheduled requests. The load generator, the application and PostgreSQL share the same machine.

## Capacity

| cell | hot | capacity req/s | per repetition | SLO missed in | hard failure | ledger checks |
|---|---|---|---|---|---|---|
| conditional-update-hot00 | 0% | 1,000 | 1,000 | 1/1 | none | all passed |
| conditional-update-hot90 | 90% | 500 | 500 | 1/1 | none | all passed |

Capacity is the median over repetitions, with its 95% bootstrap interval in brackets when the repetitions differ. Step granularity bounds it: a capacity of R means between R and the next step.

## Steps: conditional-update-hot00

| offered req/s | runs | within SLO | created/s | p50 ms | p99 ms | p99.9 ms | created p99 ms | errors | timeouts | 503 | heap MB | app CPU | generator CPU | send lag p99 ms | drain s |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 250 | 1 | 1/1 | 250 | 3 | 7 | 22 | 7 | 0.00% | 0 | 0 | 616 | 0.10 | 0.03 | 0 | 0.0 |
| 500 | 1 | 1/1 | 500 | 2 | 5 | 18 | 5 | 0.00% | 0 | 0 | 644 | 0.16 | 0.05 | 0 | 0.0 |
| 1,000 | 1 | 1/1 | 1,000 | 4 | 19 | 40 | 19 | 0.00% | 0 | 0 | 644 | 0.25 | 0.07 | 1 | 0.0 |
| 1,500 | 1 | 0/1 | 182 | 5,000 | 5,000 | 5,000 | 4,851 | 87.37% | 122,534 | 0 | 1,008 | 0.46 | 0.13 | 53 | 3.9 |
| 2,000 | 1 | 0/1 | 161 | 5,000 | 5,000 | 5,000 | 4,901 | 91.63% | 169,553 | 0 | 997 | 0.41 | 0.16 | 77 | 3.9 |
| 2,500 | 1 | 0/1 | 122 | 5,000 | 5,000 | 5,000 | 4,876 | 94.91% | 187,175 | 0 | 1,013 | 0.38 | 0.26 | 120 | 4.9 |
| 3,000 | 1 | 0/1 | 120 | 5,000 | 5,000 | 5,000 | 4,898 | 95.82% | 192,139 | 0 | 1,004 | 0.36 | 0.28 | 100 | 4.4 |
| 4,000 | 1 | 0/1 | 119 | 5,000 | 5,000 | 5,000 | 4,869 | 96.91% | 184,384 | 0 | 1,003 | 0.36 | 0.28 | 108 | 4.0 |

## Steps: conditional-update-hot90

| offered req/s | runs | within SLO | created/s | p50 ms | p99 ms | p99.9 ms | created p99 ms | errors | timeouts | 503 | heap MB | app CPU | generator CPU | send lag p99 ms | drain s |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 250 | 1 | 1/1 | 250 | 3 | 13 | 92 | 13 | 0.00% | 0 | 0 | 617 | 0.10 | 0.02 | 0 | 0.0 |
| 500 | 1 | 1/1 | 500 | 3 | 20 | 69 | 20 | 0.00% | 0 | 0 | 642 | 0.15 | 0.04 | 0 | 0.0 |
| 750 | 1 | 0/1 | 104 | 5,000 | 5,000 | 5,000 | 4,970 | 85.61% | 68,362 | 0 | 1,015 | 0.36 | 0.07 | 2 | 14.5 |
| 1,000 | 1 | 0/1 | 24 | 5,000 | 5,000 | 5,000 | 4,959 | 97.53% | 93,498 | 0 | 1,003 | 0.26 | 0.08 | 6 | 10.6 |
| 1,500 | 1 | 0/1 | 31 | 5,000 | 5,000 | 5,000 | 4,939 | 97.84% | 135,573 | 0 | 1,005 | 0.31 | 0.12 | 25 | 10.1 |
| 2,000 | 1 | 0/1 | 25 | 5,000 | 5,000 | 5,000 | 4,947 | 98.69% | 173,112 | 0 | 1,017 | 0.29 | 0.26 | 63 | 13.7 |

Each step value is the median over the repetitions that reached that step. Latencies equal to the client timeout (PT5S) mean at or beyond it. CPU columns are the mean share of all logical CPUs.
