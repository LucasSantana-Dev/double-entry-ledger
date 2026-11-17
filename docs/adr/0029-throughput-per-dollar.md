# 0029. Throughput per dollar on a simulated 2 vCPU machine

- Status: accepted
- Date: 2026-11-16

## Context

The cost map (ADR 0026) needs throughput for the machines it lists. The phase 2 benchmark ran on a 4 vCPU GitHub runner (ADR 0013). The candidate machines have 2 vCPU: t4g.small, t4g.medium, Lightsail 2 GB and 4 GB, B2als v2.

## Options

1. Run the conditional-update cells (0%, 30% and 90% hot, ADR 0012) with the application limited to 2 CPUs and the lean memory settings of ADR 0027, then divide created transfers per second by each machine's monthly price.
2. Run the full phase 2 grid of 24 cells again with the lean settings, then the same division.

## Evidence

- Conditional-update is the default strategy (ADR 0020) and led every phase 2 cell; three cells take about 20 minutes on CI, the full grid about 40 minutes plus analysis.
- On one runner, the JVM can be told to see 2 CPUs (`-XX:ActiveProcessorCount=2`) and the process can be pinned to 2 cores; PostgreSQL shares the runner, as in phase 2.

## Decision

Option 1, decided by the repo owner. Results go to `docs/benchmarks/<date>/` like phase 2, with the run id, and the cost map shows created transfers per second, per dollar a month.

## Consequences

- The numbers describe a simulated 2 vCPU machine on a shared runner, not a real t4g or B-series instance; the cost map says so.
- Arm (t4g) and x86 runners differ; the runner is x86, which the cost map also states.
- Only one strategy is measured; the phase 2 grid stays the comparison between strategies.
