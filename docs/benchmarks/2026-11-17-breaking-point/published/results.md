# Breaking-point results

Run `44012378897` started 2026-11-17T03:45:28.835582240Z on AMD EPYC 9V45 96-Core Processor (4 logical CPUs), Linux 7.0.0-1019-azure, Java 25.0.5+8-LTS. Strategy conditional-update, transfers only, 5 repetitions in shuffled order. Steps of PT2M after a PT30S warm-up at 250 req/s; client timeout PT5S; the ladder stops after 2 steps in a row outside the SLO (0: never).

SLO (ADR 0030): p99 <= 500 ms over every scheduled request, measured from its scheduled send time, and errors (5xx, timeouts, connection errors, unsent) <= 1% of scheduled requests. The load generator, the application and PostgreSQL share the same machine.

## Capacity

| cell | hot | capacity req/s | per repetition | SLO missed in | hard failure | ledger checks |
|---|---|---|---|---|---|---|
| conditional-update-hot00 | 0% | >= 1,500 (ladder too short in 5 repetitions) | 1,500, 1,500, 1,500, 1,500, 1,500 | 0/5 | none | all passed |
| conditional-update-hot90 | 90% | 600 [550, 600] | 550, 600, 600, 550, 600 | 5/5 | none | all passed |

Capacity is the median over repetitions, with its 95% bootstrap interval in brackets when the repetitions differ. Step granularity bounds it: a capacity of R means between R and the next step.

## Steps: conditional-update-hot00

| offered req/s | runs | within SLO | created/s | p50 ms | p99 ms | p99.9 ms | created p99 ms | errors | timeouts | 503 | heap MB | app CPU | generator CPU | send lag p99 ms | drain s |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 1,000 | 5 | 5/5 | 1,000 | 2 | 23 | 60 | 23 | 0.00% | 0 | 0 | 652 | 0.25 | 0.06 | 1 | 0.0 |
| 1,100 | 5 | 5/5 | 1,100 | 2 | 8 | 15 | 8 | 0.00% | 0 | 0 | 658 | 0.24 | 0.07 | 0 | 0.0 |
| 1,200 | 5 | 5/5 | 1,200 | 3 | 10 | 15 | 10 | 0.00% | 0 | 0 | 661 | 0.24 | 0.07 | 0 | 0.0 |
| 1,300 | 5 | 5/5 | 1,300 | 4 | 16 | 45 | 16 | 0.00% | 0 | 0 | 660 | 0.24 | 0.07 | 1 | 0.0 |
| 1,400 | 5 | 5/5 | 1,400 | 4 | 22 | 44 | 22 | 0.00% | 0 | 0 | 656 | 0.24 | 0.07 | 1 | 0.0 |
| 1,500 | 5 | 5/5 | 1,500 | 4 | 35 | 102 | 35 | 0.00% | 0 | 0 | 659 | 0.24 | 0.08 | 1 | 0.0 |

## Steps: conditional-update-hot90

| offered req/s | runs | within SLO | created/s | p50 ms | p99 ms | p99.9 ms | created p99 ms | errors | timeouts | 503 | heap MB | app CPU | generator CPU | send lag p99 ms | drain s |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 500 | 5 | 5/5 | 500 | 2 | 36 | 88 | 36 | 0.00% | 0 | 0 | 629 | 0.13 | 0.03 | 0 | 0.0 |
| 550 | 5 | 5/5 | 550 | 2 | 3 | 19 | 3 | 0.00% | 0 | 0 | 654 | 0.12 | 0.03 | 0 | 0.0 |
| 600 | 5 | 3/5 | 600 | 2 | 320 | 457 | 320 | 0.00% | 0 | 0 | 654 | 0.13 | 0.03 | 0 | 0.0 |
| 650 | 5 | 0/5 | 515 | 853 | 5,000 | 5,000 | 4,863 | 20.71% | 16,154 | 0 | 1,001 | 0.17 | 0.06 | 0 | 0.0 |
| 700 | 3 | 3/3 | 700 | 2 | 4 | 22 | 4 | 0.00% | 0 | 0 | 831 | 0.15 | 0.05 | 0 | 0.0 |
| 750 | 3 | 0/3 | 728 | 2 | 5,000 | 5,000 | 4,455 | 2.95% | 2,654 | 0 | 980 | 0.17 | 0.06 | 0 | 1.5 |

Each step value is the median over the repetitions that reached that step. Latencies equal to the client timeout (PT5S) mean at or beyond it. CPU columns are the mean share of all logical CPUs.
