# 0013. Published numbers come from GitHub Actions, 5 shuffled repetitions in one job

- Status: accepted
- Date: 2026-11-13

## Context

Benchmark numbers in the README must be believable and reproducible by a reader. Load generator, application and PostgreSQL share one machine in every option below, and the results state this as a limit.

## Options

1. GitHub Actions `ubuntu-latest`, manual workflow separate from CI, 24 cells x 5 repetitions in shuffled order inside one job (about 3 hours), median and 95% bootstrap confidence interval per cell.
2. The owner's desktop (Ryzen 7 9800X3D, 16 threads, 31 GB, Docker Desktop on WSL2), 5 repetitions.
3. A dedicated cloud VM.
4. Desktop to iterate, Actions to publish.

## Evidence

- GitHub documentation: standard Linux runners for public repositories have 4 vCPU, 16 GB RAM, and are free.
- CodSpeed measured a 2.66% coefficient of variation on GitHub-hosted runners over 100 runs, against 0.56% on bare metal, and saw different CPU models behind the same runner image across jobs.
- Quansight Labs recommends relative benchmarking on shared CI: compare variants inside the same job, on the same machine, rather than absolute timings across jobs.

## Decision

Option 1. All cells of a run share one job and so one CPU model; shuffling the order spreads slow periods of the machine across cells instead of concentrating them on one.

## Consequences

- Absolute numbers are small (4 vCPU) and only comparable within one run. The README reports the run id, the CPU model read from `/proc/cpuinfo` and the date.
- One full run takes about 3 hours of the 6 hour job limit, so a run is triggered by hand, never on every push.
- The desktop is still used while developing the plan, but its numbers are never published.
