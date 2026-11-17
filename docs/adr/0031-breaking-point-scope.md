# 0031. Breaking-point scope: conditional-update, transfers only, 0% and 90% hot, 5 repetitions

- Status: accepted
- Date: 2026-11-16

## Context

ADR 0030 fixed what breaking means, the load model and the machine. Four choices remained: the operation mix, the hot-account shares, the strategies, and the repetitions.

## Options

Mix: transfers only; 80% transfers and 20% balance reads; 70% transfers, 20% reads and 10% deposits.
Hot share: 0% and 90%; 0%, 30% and 90%; 0% only.
Strategies: conditional-update only; conditional-update and pessimistic; all four.
Repetitions of the published run: 5; 3; 1.

## Evidence

- Phase 2, 256 users (`docs/benchmarks/2026-11-13/results.md`): conditional-update created 2,042, 1,602 and 983 transfers/s at 0%, 30% and 90% hot, and led every cell (ADR 0020).
- Optimistic and serializable answered 503 to 11% to 13% of requests at 30% hot and 45% to 48% at 90% hot, above the 1% error objective at any rate with contention. At 0% hot the four strategies were within 1,557 to 2,042 created/s.
- Every deposit debits the settlement account (ADR 0008), so deposits add a hot row of their own; a balance read takes no lock but holds one of the 10 pool connections.
- Each cell takes about 15 minutes per repetition (steps of 2 minutes plus start, seed, warm-up and checks). With 2 cells and 5 repetitions that is about 2.5 hours, inside the 6 hour job limit. Phase 2 used 5 shuffled repetitions with a 95% bootstrap interval (ADR 0013).

## Decision

Decided by the repo owner on 2026-11-16: conditional-update only, transfers only, 0% and 90% hot, 5 repetitions in shuffled order after one calibration run with the coarse ladder.

## Consequences

- One request is one transaction, so the capacity answers both "requests per second" and "transactions per second".
- The two hot shares bound the range: no contention and the worst contention phase 2 measured. 30% is left out.
- The strategy comparison stays the phase 2 grid; this run says how far the default goes.
- A read-heavy mix would likely show a higher request rate; that is not measured here.
