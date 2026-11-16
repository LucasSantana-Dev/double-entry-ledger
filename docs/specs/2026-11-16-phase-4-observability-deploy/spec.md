---
status: planned
date: 2026-11-16
phase: 4
tags: [tracing, metrics, opentelemetry, grafana, kubernetes, kind, write-up]
---

# Phase 4: traces, metrics, deployment, results write-up

## Goal

Make the ledger's behavior visible and runnable outside the test suite. A transfer shows up as one trace from the HTTP request to the statement line, across the asynchronous outbox relay. The outbox, the conflicts per strategy and the reconciliation show up as metrics on a dashboard. The service runs in Kubernetes, deployed and smoke-tested in CI. As in phases 1 to 3, each claim is backed by a test, not asserted.

## Scope

1. Tracing (ADR 0021): `spring-boot-starter-opentelemetry`, OTLP export, `spring.kafka.template.observation-enabled` and `spring.kafka.listener.observation-enabled` set to true, `datasource-micrometer-spring-boot` for JDBC spans. Flyway V5 adds a nullable `traceparent` column to `outbox_events`, filled from the current span in the transfer transaction. The relay sends each row in a span whose parent is the stored context, and a new root span when there is none. Sampling 1.0 in tests and the local stack.
2. Metrics (ADR 0022): `ledger.outbox.pending`, `ledger.outbox.oldest.pending.age`, `ledger.outbox.parked` refreshed by one query on a schedule; `ledger.transfer.retries` and `ledger.transfer.unavailable` tagged `strategy`; `ledger.reconciliation.differences` from the last run; the existing `ledger.statement.dead-lettered`. A percentile histogram on `http.server.requests`. No tag with an account, transfer or key id.
3. Logs (ADR 0022): ECS JSON on the console with trace and span ids. No OTLP log export.
4. Local stack (ADR 0022): `compose.yaml` with the application, PostgreSQL 18, Kafka and `grafana/otel-lgtm` at a pinned tag; one provisioned ledger dashboard (requests by status and p99, outbox, conflicts by strategy, consumer lag, reconciliation, dead letters) as JSON in the repo.
5. Observability tests (ADR 0023): in-process span and meter assertions, one `LgtmStackContainer` smoke test with `@ServiceConnection`.
6. Container and deployment (ADR 0024): multi-stage `Dockerfile` (JDK 25 build, JRE 25 runtime, non-root); `deploy/k8s/base` (application Deployment and Service, PostgreSQL 18 and single-node KRaft Kafka StatefulSets, topic setup, ConfigMap, Secret) and `deploy/k8s/overlays/kind`; liveness and readiness from Actuator health groups; a GitHub Actions workflow that builds the image, creates a kind cluster, applies the overlay, waits for the rollout and runs a smoke test.
7. Write-up (ADR 0025): final README with the results of phases 1 to 4 and a trace and dashboard screenshot; an article draft in English under `docs/writeup/`.

Out of scope: a public deployment or any cloud account, Terraform, Helm, the Strimzi operator, OTLP log export, the OpenTelemetry Java agent, alerting rules, a production-grade observability backend, several application replicas (the single relay of ADR 0016 still holds), publishing the article.

## Acceptance criteria

1. One committed transfer yields one trace (one trace id) containing the HTTP server span, at least one JDBC span, the relay send span, and the consumer span, in that parent order: the send span's parent is the context stored in the outbox row, and the consumer span is a child of the send span.
2. The Kafka record of a transfer carries a W3C `traceparent` header with that trace id.
3. A transfer committed while tracing was not sampled, or an outbox row without a `traceparent`, is still published and applied (tracing never blocks the outbox).
4. While Kafka is paused, `ledger.outbox.pending` and `ledger.outbox.oldest.pending.age` rise; after it resumes and the relay drains, pending returns to 0.
5. Under the optimistic and serializable strategies with a hot account, `ledger.transfer.retries` counts the retries with the right `strategy` tag, and `ledger.transfer.unavailable` equals the number of 503 responses.
6. After a reconciliation with no differences, `ledger.reconciliation.differences` is 0.
7. With `LgtmStackContainer`, a transfer's trace and a ledger metric reach the backend over OTLP.
8. In CI, the kind deployment rolls out and the smoke test (account, deposit, transfer, statement line) passes.
9. The five phase 3 invariants and the seven phase 1 invariants stay green for all four strategies, and `FaultInjectionTest` stays green. The `verify` job stays under its 20 minute limit; the added time is measured and recorded in ADR 0023.

## How the tests check them

| Criterion | Test |
|---|---|
| 1, 2, 3 | `TracingTest` (new): in-process spans and the record header; a row with a null `traceparent` |
| 4 | `ObservabilityMetricsTest` (new), Kafka paused through the Docker API as in `FaultInjectionTest` |
| 5 | `ObservabilityMetricsTest`, optimistic and serializable with a hot account |
| 6 | `ReconciliationTest` extended, or `ObservabilityMetricsTest` |
| 7 | `OtlpExportSmokeTest` (new) with `LgtmStackContainer` |
| 8 | the kind workflow's smoke script |
| 9 | `./mvnw verify` on CI, timed |

## Decisions

| ADR | Decision |
|---|---|
| 0021 | Boot's OpenTelemetry starter, Kafka observation, JDBC spans, `traceparent` stored in the outbox row and restored as the send span's parent |
| 0022 | `grafana/otel-lgtm` in compose, OTLP metrics and traces, custom outbox, conflict and reconciliation meters, ECS JSON logs |
| 0023 | In-process span and meter assertions, one OTLP smoke test with `LgtmStackContainer` |
| 0024 | Dockerfile, Kustomize manifests with PostgreSQL and KRaft Kafka StatefulSets, kind in GitHub Actions, no public link |
| 0025 | Final README and an article draft in the repo |

Evidence for the decisions: [docs/market/2026-11-16-gupy-phase-4-keywords.md](../../market/2026-11-16-gupy-phase-4-keywords.md) (`tools/market/gupy_phase4_keywords.py`) and the sources cited in each ADR.

## Done when

- `./mvnw verify` passes with criteria 1 to 7 and 9; the kind workflow passes criterion 8.
- ADR 0023 records the measured CI cost.
- `docker compose up` brings up the application and Grafana with the ledger dashboard and traces, as described in the README.
- README carries the phase 4 results, the decision table rows 0020 to 0025, and the roadmap shows phase 4 done; the article draft is in `docs/writeup/`.
