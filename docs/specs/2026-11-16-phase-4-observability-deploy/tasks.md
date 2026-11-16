# Phase 4 tasks

- [x] Spec and ADRs 0021 to 0025 from the decisions taken on 2026-11-16
- [x] Gupy sample with phase 4 keywords (`tools/market/gupy_phase4_keywords.py`, `docs/market/2026-11-16-gupy-phase-4-keywords.md`)
- [ ] Tracing dependencies and properties; V5 migration with `outbox_events.traceparent`; relay restores the context; `TracingTest`
- [ ] Custom meters (outbox, conflicts by strategy, reconciliation); percentile histogram; ECS JSON logs; `ObservabilityMetricsTest`
- [ ] `LgtmStackContainer` with `@ServiceConnection`; `OtlpExportSmokeTest`
- [ ] `compose.yaml` with `grafana/otel-lgtm` and the provisioned ledger dashboard
- [ ] `Dockerfile`; `deploy/k8s/base` and `overlays/kind`; kind workflow with the smoke script
- [ ] Measure the CI cost and record it in ADR 0023
- [ ] README: phase 4 results, screenshots, decision rows 0021 to 0025, roadmap; article draft in `docs/writeup/`; spec status done
