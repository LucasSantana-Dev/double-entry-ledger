---
status: in-progress
date: 2026-11-16
phase: 6
tags: [benchmark, capacity, slo, open-model, coordinated-omission]
---

# Breaking point: the highest arrival rate the ledger serves within its SLO

## Goal

Answer "how many transactions and requests per second does the ledger take before it breaks?" with a number that holds up: the highest arrival rate at which the service still meets its SLO, measured with an open load model so a slow server cannot slow the load down and hide its own latency.

## Scope

1. An open-model load generator under `src/benchmark` (ADR 0032): requests leave at a fixed interval for the step's rate, each on its own virtual thread; latency runs from the scheduled send time to the answer; a client timeout counts as an error.
2. A breaking-point run (ADR 0030, ADR 0031): conditional-update strategy, transfers only, 0% and 90% hot, a ladder of arrival-rate steps of 2 minutes each after a discarded warm-up. After each step: the in-flight requests and the server drain, the ledger invariants are checked, and one JSON line per step is written.
3. A manual GitHub Actions workflow for the run, separate from CI and from the phase 2 benchmark; the test stays out of `./mvnw verify` and out of the phase 2 `-Pbenchmark` run.
4. An aggregator (stdlib Python) that turns the step lines into a capacity per cell (median and 95% bootstrap interval over repetitions) and a per-step table.
5. A calibration run with the coarse ladder of ADR 0030 and no early stop, then the published run with 5 repetitions and a ladder refined around the knee the calibration found.
6. Results under `docs/benchmarks/<date>/`, a section in `docs/results.md`, one line in the README.

Out of scope: other strategies, deposits and reads in the mix, the 30% hot share, the phase 4 observability stack, Kafka publishing, tuning the application to raise the number.

## Definitions (ADR 0030)

- **Step**: a fixed arrival rate held for 2 minutes. Requests are scheduled at `start + i / rate`.
- **Latency** of a request: answer time minus scheduled send time. Timeouts and connection errors count as infinitely slow.
- **Error**: a 5xx answer, a client timeout, a connection error, or a request the generator could not send.
- **Step passes the SLO** when p99 latency over every scheduled request is at most 500 ms, errors are at most 1% of scheduled requests, the application survived the step, and the server drained its backlog within 2 minutes after it.
- **4xx answers** are not expected (every request is valid and every account funded); any 4xx fails the run as a harness bug.
- **Capacity** of a repetition: the rate of the last step that passed before the first step that failed. The published capacity of a cell is the median over repetitions.
- **Hard failure**: the application process exits (for example out of memory) or stops accepting connections. Recorded with the step where it happened.

## Acceptance criteria

1. The generator's latency is measured from the scheduled time, verified by a check against a stub server: when the generator itself stalls for one second, every request scheduled during the stall reports the delay as latency (no coordinated omission).
2. Every step line carries offered rate, scheduled, completed, created per second, status counts, timeouts, connection errors, unsent requests, p50, p90, p99, p99.9 and max latency, created-only p99, SLO verdict, ledger check result, application alive, peak heap, mean CPU of the application and of the generator, and drain time.
3. The ledger invariants (sum of balances zero, no negative customer balance, balances equal their entries) are checked after every step; a violation fails the run.
4. The calibration run and the published run go through GitHub Actions on the 4 vCPU runner, with the run ids recorded.
5. `./mvnw verify` stays green and does not run the breaking-point test; the phase 2 benchmark workflow still selects only its own test.
6. The report states that the generator, the application and PostgreSQL share one 4 vCPU machine.

## Decisions

| ADR | Decision |
|---|---|
| 0030 | Breaking point = last step within the SLO (p99 <= 500 ms, errors <= 1%), open model in arrival-rate steps, 4 vCPU GitHub runner |
| 0031 | Conditional-update, transfers only, 0% and 90% hot, 5 repetitions after one calibration run |
| 0032 | Own open-model generator in Java, latency from the scheduled time |

## Done when

- Criteria 1 to 6 hold.
- Results, the `docs/results.md` section and the README line are committed on `feat/breaking-point`, and the PR is open with CI green.
