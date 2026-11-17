# 0033. Admission control: a concurrency limiter in front of transfers and deposits

- Status: accepted
- Date: 2026-11-17

## Context

The breaking-point runs (phase 6, ADR 0030) showed the service collapsing past its capacity: up to 8,192 requests, each on a virtual thread, waited up to 30 s for 10 database connections while the client gave up at 5 s, and goodput fell to 74 to 182 created transfers per second against 1,500 offered. Phase 7 (`docs/specs/2026-11-17-admission-control`) measured two ways of bounding that queue against the application as it was, with every arm of a repetition on the same runner.

## Options

1. **None.** Keep the application as it is and document the collapse.
2. **Pool timeout.** `spring.datasource.hikari.connection-timeout=1000`: a request waits at most 1 s for a connection, below the 5 s client timeout. Configuration only.
3. **Limiter.** A servlet filter (`AdmissionFilter`) that admits at most 16 `POST /transfers` and `POST /deposits` at a time and answers `503` with `Retry-After: 1` and an RFC 9457 problem (`/problems/overloaded`) when no slot frees within 100 ms.
4. **Limiter and pool timeout together.** Not measured as an arm of its own.

## Evidence

Run 44037483385, 5 GitHub-hosted 4 vCPU runners (Intel Xeon 6975P-C, AMD EPYC 9V45, Intel Xeon Platinum 8573C, two AMD EPYC 9V74), results in [`docs/benchmarks/2026-11-17-admission-control`](../benchmarks/2026-11-17-admission-control/results.md).

| | baseline | pool timeout | limiter |
|---|---|---|---|
| jump to 1,500 req/s, 0% hot, first 2 min: created/s (range over 5 runners) | 130 to 1,500 | 954 to 1,464 | 1,176 to 1,485 |
| same step: p99 | 914 ms to the 5 s timeout | 1,027 to 1,163 ms | 116 to 145 ms |
| same step: how requests fail | client timeouts at 5 s (up to 126,027 per step) | 5xx without `Retry-After` | `503` with `Retry-After` |
| recovery probe at 1,000 req/s after the jump | 5 of 5 | 4 of 5 (one at p99 2.8 s) | 5 of 5 |
| 650 req/s at 90% hot for 10 minutes: created/s | 12 to 633 (4 of 5 collapsed) | 564 to 638 | 588 to 637 |
| same hold: seconds with p99 above 500 ms, median per 2 minute window | 120 of 120 | 0 to 75 | 0 |
| peak heap in the jump | about 1,000 MB (the 1 GB limit) | about 900 MB | about 670 MB |

Paired within each runner, the limiter created more transfers than the pool timeout in the first 2 minutes of the jump in 5 of 5 runners (by 21 to 222 per second) and had the lower p99 in 5 of 5. Every step of every arm passed the ledger checks, with no 4xx.

Cost observed: near capacity the limiter rejects some requests the baseline would have served within the SLO. On one EPYC 9V74 the baseline held 1,200 req/s with no errors and the limiter rejected 1.4% of them; on two runners it rejected 0.1 to 0.3% in the first step after the warm-up.

## Decision

Option 3, the limiter, on by default with 16 slots and a 100 ms wait, decided by the repo owner on 2026-11-17.

## Consequences

- Past capacity the service answers fast `503`s with `Retry-After` and keeps creating transfers at about its capacity, instead of collapsing to a tenth of it. It does not raise the capacity.
- A client sees `503` under overload and should retry after the hinted delay with the same idempotency key, which ADR 0006 already makes safe.
- The 16 slots are tied to the pool of 10 connections. Changing the pool size means revisiting the limit; `ledger.admission.max-concurrent=0` switches the filter off.
- Reads, the outbox relay and the statement consumer do not go through the filter and still wait up to 30 s for a connection. The pool timeout (option 4) would cover them and is left for when a run shows them queueing.
- The benchmarks of phases 2 to 6 and the `baseline` and `hikari` arms start the application with the filter off, so their numbers stay comparable with the published ones.
- `ledger.admission.rejected` counts the rejections.
