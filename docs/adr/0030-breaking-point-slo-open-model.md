# 0030. Breaking point: last arrival-rate step within the SLO, open model, 4 vCPU runner

- Status: accepted
- Date: 2026-11-16

## Context

The repo owner asked how many transactions and requests per second the ledger takes before it breaks. Phase 2 (ADRs 0012 to 0015) measured throughput with a closed model: N users, each sending the next request when the last one answered. At 256 users conditional-update created 2,042 transfers/s at 0% hot with p99 354 ms; at 1,024 users p99 was 1.0 to 1.6 s with no errors (`docs/benchmarks/2026-11-13/results.md`). A closed model cannot find a breaking point: when the server slows down, the users slow down with it, so the arrival rate drops exactly when it should not, and the waiting time of requests that were never sent is not measured (coordinated omission, ADR 0012 consequences).

By design the ledger does not corrupt under load: a conflict becomes 503 with Retry-After and nothing is written half way. The expected failure is latency, queueing and memory, not wrong balances.

## Options

What "breaks" means:

1. SLO: the capacity is the last step with p99 <= 500 ms and errors (5xx and timeouts) <= 1%.
2. Hard failure only: the rate at which the process dies or refuses connections.
3. Throughput plateau: the rate past which created transfers per second stop growing.

How load is applied:

1. Open model: a fixed arrival rate raised in steps (start: 250, 500, 1,000, 1,500, 2,000, 3,000 req/s, 2 minutes each, refined after a first run); latency measured from the scheduled send time.
2. Closed model with more users, as in phase 2.

Where:

1. GitHub Actions `ubuntu-latest`, 4 vCPU, the runner of phases 2 and 5.
2. The owner's desktop.

## Evidence

- Gil Tene, "How NOT to Measure Latency" (2015) and the wrk2 README: a load generator that waits for answers before sending omits the latency of the requests it should have sent; measuring from the intended send time corrects it.
- Google SRE book, chapter 4 "Service Level Objectives": capacity is useful when stated against a latency and error objective, not as raw throughput.
- Phase 2 and phase 5 ran on the same runner type (ADR 0013, ADR 0029), so the numbers can be set side by side within the limits ADR 0013 states.

## Decision

Decided by the repo owner on 2026-11-16:

1. Breaking point = SLO: p99 <= 500 ms over every scheduled request and errors <= 1%. The hard failure, if it happens (out of memory, refused connections), is recorded with its step, and the ledger invariants are checked after every step.
2. Open model in arrival-rate steps, latency from the scheduled send time. The ladder above is the starting point; a calibration run sets the published ladder.
3. GitHub Actions 4 vCPU runner. Generator, application and PostgreSQL share the VM, and the report says so.

## Consequences

- The headline is a rate the service sustains within its SLO, not a peak it touched once.
- Step granularity bounds precision: a capacity of 1,500 means "between 1,500 and the next step". The refined ladder narrows that near the knee.
- On a shared 4 vCPU VM the generator competes for CPU with the server; its CPU is reported per step. A dedicated generator machine would give a higher number.
- 500 ms and 1% are this project's objective, not an industry standard; the per-step table lets a reader apply a different one.
