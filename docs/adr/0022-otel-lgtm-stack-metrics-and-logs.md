# 0022. Local observability stack, custom metrics and structured logs

- Status: accepted
- Date: 2026-11-16

## Context

Traces (ADR 0021) and metrics need a backend and dashboards that a reviewer can start with one `docker compose up`. The application also needs metrics of its own, because the stock ones do not show the outbox, the conflicts or the reconciliation.

## Options

1. `grafana/otel-lgtm` (OpenTelemetry Collector, Prometheus, Tempo, Loki, Grafana in one image), metrics and traces pushed over OTLP, dashboards as code.
2. Separate Prometheus, Grafana and Tempo containers, metrics scraped from `/actuator/prometheus`.
3. Prometheus and Grafana for metrics, Jaeger v2 for traces.

## Evidence

- `grafana/otel-lgtm` is "intended for development, demo, and testing environments", needs about 2 GB of memory, and loads provisioned dashboards from `/otel-lgtm/grafana/conf/provisioning/dashboards` ([docker-otel-lgtm](https://github.com/grafana/docker-otel-lgtm)). Listings seen before the decision put the image at 470 to 560 MB; the pinned 0.40.0 measured about 956 MB compressed during implementation.
- Spring Boot 4.1.2 creates OTLP metrics, tracing and logging connection details from `LgtmStackContainer` through `@ServiceConnection` ([Boot Testcontainers reference](https://docs.spring.io/spring-boot/reference/testing/testcontainers.html)), so tests and the compose file use the same backend.
- The Prometheus scrape endpoint needs the Prometheus registry and an exposure change ([Boot metrics](https://docs.spring.io/spring-boot/reference/actuator/metrics.html)). Option 2 roughly triples the compose and provisioning configuration (three services, datasources, Tempo config). Jaeger keeps traces outside Grafana, so a latency panel cannot jump to a trace.
- Boot 4 has native structured logging (`logging.structured.format.console=ecs`) with the MDC keys, trace and span ids included, in every line ([Boot logging](https://docs.spring.io/spring-boot/reference/features/logging.html)). OTLP log export needs the alpha Logback appender wired by hand ([Boot loggers](https://docs.spring.io/spring-boot/reference/actuator/loggers.html)).
- Market (Gupy, 100 Java postings, 2026-11-16, [docs/market/2026-11-16-gupy-phase-4-keywords.md](../market/2026-11-16-gupy-phase-4-keywords.md)): Grafana 10%, Prometheus 5%, observability or monitoring in any sense 46%.

## Decision

Option 1, decided by the repo owner.

- `compose.yaml` with the application, PostgreSQL 18, Kafka and `grafana/otel-lgtm` at a pinned tag.
- Metrics and traces over OTLP to the collector; logs as ECS JSON on the console with trace and span ids. No OTLP log export.
- Custom meters, none tagged with an account, transfer or key id:
  - outbox: `ledger.outbox.pending`, `ledger.outbox.oldest.pending.age` (seconds), `ledger.outbox.parked`;
  - conflicts: `ledger.transfer.retries` and `ledger.transfer.unavailable` (the 503s), tagged `strategy`;
  - reconciliation and dead letters: `ledger.reconciliation.differences` from the last run, and the existing `ledger.statement.dead-lettered`.
- The outbox gauges come from one query per scheduled refresh, not one per export.
- `http.server.requests` with a percentile histogram, so p99 per status matches how phase 2 reported latency.
- One provisioned dashboard for the ledger story (requests by status and p99, outbox, conflicts by strategy, consumer lag, reconciliation, dead letters) next to the stock JVM panels.

## Consequences

- One container gives Grafana, metrics, traces and logs on `localhost:3000`, but it is a demo stack, and the README says so.
- About 2 GB more memory on the machine running the compose file.
- Metric names reach Prometheus through OTLP conversion (dots to underscores, unit suffixes); the dashboard queries use the converted names.
