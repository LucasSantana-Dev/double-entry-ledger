# Admission-control results

5 jobs, one repetition each; every cell and arm of a repetition ran on the same runner. CPU per repetition: rep 1: Intel(R) Xeon(R) 6975P-C, rep 2: AMD EPYC 9V74 80-Core Processor, rep 3: INTEL(R) XEON(R) PLATINUM 8573C, rep 4: AMD EPYC 9V45 96-Core Processor, rep 5: AMD EPYC 9V74 80-Core Processor.

## Path effect (same runner)

| arm | rep | climb at 1,500 within SLO | jump to 1,500 within SLO | climb created/s | jump created/s |
|---|---|---|---|---|---|
| baseline | 1 | True | False | 1,500 | 1,499 |
| baseline | 2 | False | False | 307 | 150 |
| baseline | 3 | False | False | 402 | 199 |
| baseline | 4 | True | False | 1,500 | 1,500 |
| baseline | 5 | False | False | 279 | 130 |
| hikari | 1 | True | False | 1,500 | 1,452 |
| hikari | 2 | False | False | 1,156 | 1,150 |
| hikari | 3 | False | False | 1,297 | 1,297 |
| hikari | 4 | True | False | 1,500 | 1,464 |
| hikari | 5 | False | False | 1,139 | 954 |
| limiter | 1 | True | False | 1,500 | 1,480 |
| limiter | 2 | False | False | 1,238 | 1,255 |
| limiter | 3 | False | False | 1,366 | 1,397 |
| limiter | 4 | True | True | 1,500 | 1,485 |
| limiter | 5 | False | False | 1,168 | 1,176 |

## Arms under the jump to 1,500 req/s

Medians over repetitions; the difference column is the median of the per-job differences against baseline.

| arm | step | within SLO | created/s | vs baseline | p99 ms | errors | 503 | timeouts | heap MB |
|---|---|---|---|---|---|---|---|---|---|
| baseline | 1,500 (first 2 min) | 0/5 | 199 |  | 5,000 | 86.72% | 0 | 120,444 | 1,012 |
| baseline | 1,500 (next 2 min) | 2/5 | 1,467 |  | 5,000 | 2.17% | 0 | 3,902 | 963 |
| baseline | probe at 1,000 | 5/5 | 1,000 |  | 13 | 0.00% | 0 | 0 | 929 |
| hikari | 1,500 (first 2 min) | 0/5 | 1,297 | +825 | 1,078 | 13.52% | 0 | 0 | 906 |
| hikari | 1,500 (next 2 min) | 2/5 | 1,446 | +0 | 1,005 | 3.63% | 0 | 0 | 907 |
| hikari | probe at 1,000 | 4/5 | 1,000 | +0 | 8 | 0.00% | 0 | 0 | 852 |
| limiter | 1,500 (first 2 min) | 1/5 | 1,397 | +1,046 | 126 | 6.84% | 12,305 | 0 | 667 |
| limiter | 1,500 (next 2 min) | 3/5 | 1,495 | +28 | 112 | 0.32% | 569 | 0 | 668 |
| limiter | probe at 1,000 | 5/5 | 1,000 | +0 | 8 | 0.00% | 0 | 0 | 671 |

## Hold at 650 req/s, 90% hot, 10 minutes without a break

Per 2 minute window of the one continuous step: created/s, and seconds whose p99 passed 500 ms (median over repetitions). Requests are counted in the second they were scheduled.

| arm | step within SLO | window | created/s | slow seconds |
|---|---|---|---|---|
| baseline | 0/5 | 0-2 min | 58 | 120 |
| baseline | 0/5 | 2-4 min | 2 | 120 |
| baseline | 0/5 | 4-6 min | 2 | 120 |
| baseline | 0/5 | 6-8 min | 2 | 120 |
| baseline | 0/5 | 8-10 min | 2 | 120 |
| hikari | 0/5 | 0-2 min | 588 | 62 |
| hikari | 0/5 | 2-4 min | 650 | 0 |
| hikari | 0/5 | 4-6 min | 616 | 24 |
| hikari | 0/5 | 6-8 min | 583 | 75 |
| hikari | 0/5 | 8-10 min | 650 | 0 |
| limiter | 0/5 | 0-2 min | 592 | 0 |
| limiter | 0/5 | 2-4 min | 650 | 0 |
| limiter | 0/5 | 4-6 min | 618 | 0 |
| limiter | 0/5 | 6-8 min | 586 | 0 |
| limiter | 0/5 | 8-10 min | 650 | 0 |

Slow-second onsets (a second whose p99 passes 500 ms after one that did not) near a PostgreSQL checkpoint, an autovacuum run or a GC pause of 50 ms or more (within 5 s of the event's interval), against chance: the share of all held seconds near such an event. An observed share well above chance links the stalls to that event; near chance, it does not.

| arm | onsets | near a checkpoint (chance) | near autovacuum (chance) | near a GC pause (chance) | onsets near none | seconds without a pool sample |
|---|---|---|---|---|---|---|
| baseline | 7 | 2/7 (61%) | 0/7 (22%) | 3/7 (69%) | 2 | 17/3000 |
| hikari | 8 | 3/8 (61%) | 0/8 (22%) | 0/8 (0%) | 5 | 0/3000 |
| limiter | 0 | 0/0 (61%) | 0/0 (22%) | 0/0 (0%) | 0 | 0/3000 |

Ledger checks and 4xx: every step of every arm passed, no 4xx.

Per-step tables for every cell and arm are in summary.json (`cells`).
