# 0015. Latency reported for created transfers and for all responses, side by side

- Status: accepted
- Date: 2026-11-13

## Context

ADR 0012 reports latency percentiles per cell. JMeter's summary mixes every response: 201 (transfer created) and 503 (the strategy gave up after 5 conflict aborts, ADR 0010). In a 10 second smoke run of the serializable strategy with the hot account at 90%, 976 of 1,891 responses were 503, so its p50 of 1,515 ms described both outcomes at once. Strategies that never abort have no 503 at all, so the mixed number is not comparable across strategies.

## Options

1. Latency of created transfers as the headline, latency of all responses next to it, plus the 503 rate. Needs every sample's status and elapsed time: JMeter writes them to a JTL file, the benchmark computes percentiles per status and deletes the file.
2. Only the latency of created transfers, the 503 share as a rate. Hides how long a client waits to be told 503.
3. Keep the mixed numbers and explain them in the write-up.

## Decision

Option 1.

## Consequences

- Each cell reports `p50Ms`, `p95Ms`, `p99Ms`, `maxMs` for 201 responses and `allP50Ms` to `allMaxMs` for every response, both from the same JTL samples with the nearest-rank method.
- A strategy cannot look faster because it gives up early, nor slower because it keeps trying; the 503 count sits in the same row.
- The JTL file (up to tens of thousands of lines per cell, two fields each) lives only while its cell runs.
