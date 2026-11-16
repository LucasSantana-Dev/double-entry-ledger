# 0023. Observability tests: in process, plus one OTLP smoke test

- Status: accepted
- Date: 2026-11-16

## Context

Tracing (ADR 0021) and metrics (ADR 0022) are only claims until a test shows them. The invariant test and the fault injection test remain the contract, and CI must stay well under its 20 minute limit (it runs in about 3:30 today).

## Options

1. Assert spans and meters in process; one smoke test with `LgtmStackContainer` checks that OTLP export reaches a real backend.
2. In process only.
3. Every check against the LGTM container (query Tempo and Prometheus).

## Evidence

- `micrometer-tracing-test` provides `SimpleTracer`, `TracerAssert` and `SpansAssert` for in-process assertions ([Micrometer Tracing testing](https://docs.micrometer.io/tracing/reference/testing.html)). A Boot 4 `@SpringBootTest` recipe is not documented, so the wiring is settled during implementation, with an OpenTelemetry SDK in-memory exporter as the fallback.
- Grafana says `otel-lgtm` starts in under 5 seconds since 2025 ([Grafana blog, 2025-07-08](https://grafana.com/blog/2025/07/08/observability-in-under-5-seconds-reflecting-on-a-year-of-grafana-otel-lgtm)), a vendor figure without an independent benchmark. The pinned 0.40.0 image is about 956 MB compressed to pull in CI (measured during implementation, above the 500 MB first estimated).
- A span check against the container has to poll Tempo until the batch exporter flushes, which is slower and can flake.

## Decision

Option 1, decided by the repo owner.

- In process: one transfer yields one trace with the HTTP server span, JDBC spans, the relay send span whose parent comes from the outbox row, and the consumer span; the Kafka record carries a `traceparent` header; the custom meters of ADR 0022 move as expected (pending outbox grows while Kafka is paused, the strategy counters count retries).
- One smoke test starts `LgtmStackContainer` with `@ServiceConnection` and checks that a trace and a metric arrive.
- The CI cost of the new tests is measured and recorded here.

## Consequences

- Fast, deterministic checks for the structure of the trace; one slower test proves the export path.
- CI pulls one more image.

## Measured cost

GitHub Actions `verify` job, ubuntu-latest, 2026-11-16: 3:57 and 4:02 on main before phase 4 (runs 43962847652 and 43961482755), 5:27 on the phase 4 branch (run 43974868216, Maven `Total time` 5:11, 77 tests). Phase 4 adds about 1:30 to the job, of which `OtlpExportSmokeTest` takes 49 s including the `grafana/otel-lgtm:0.40.0` pull. Locally (Windows, Docker Desktop, image cached) the same suite went from 3:51 to 4:58. Well inside the 20 minute limit.
