# 0032. Own open-model load generator in Java, latency from the scheduled time

- Status: accepted
- Date: 2026-11-16

## Context

ADR 0030 requires latency measured from the scheduled send time. The phase 2 harness uses JMeter Java DSL (ADR 0011) in a closed model.

## Options

1. A generator in `src/benchmark`: JDK `HttpClient`, requests sent at a fixed interval, each on its own virtual thread, latency = answer time minus scheduled time, a client timeout counted as an error. About 150 lines, no new dependency.
2. JMeter `rpsThreadGroup` (already a dependency).
3. wrk2.
4. k6 `constant-arrival-rate`.

## Evidence

- JMeter and k6 time a sample from its actual send. When the generator falls behind (threads or virtual users exhausted, CPU shared with the server), the delay is not in the latency; k6 counts the missed iterations as `dropped_iterations` instead.
- wrk2 corrects coordinated omission by design (Gil Tene), but it is C with no release since 2019, would be built on the runner, and needs Lua for a fresh idempotency key and random accounts per request.

## Decision

Option 1, decided by the repo owner on 2026-11-16.

## Consequences

- The definition of latency is in this repo and covered by a check against a stub server that stalls: a stall adds its delay to every request scheduled behind it.
- The generator runs on the same VM as the server and reports its own CPU per step.
- Requests leave at an even interval, not a Poisson process; bursts within a step are not modelled.
- The phase 2 JMeter harness stays as it is for the closed-model grid.
