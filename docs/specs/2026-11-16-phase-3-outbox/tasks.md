# Phase 3 tasks

- [x] Spec and ADRs 0016 to 0019 from the decisions taken on 2026-11-16
- [x] Gupy sample with phase 3 keywords (`tools/market/gupy_phase3_keywords.py`, `docs/market/2026-11-16-gupy-phase-3-keywords.md`)
- [x] V3 migration with `outbox_events`; event written in the transfer and deposit transaction; invariant 1 in `ConcurrentTransfersInvariantTest`
- [x] Kafka dependencies, `KafkaContainer` with `@ServiceConnection` in the shared test configuration; relay with advisory lock, poison parking, cleanup; `OutboxRelayTest`
- [x] V4 migration with the `statement` schema; listener with dedup; `StatementProjectionTest`
- [x] Reconciliation job and `ReconciliationTest`; invariant test drains and reconciles for all four strategies
- [x] `FaultInjectionTest` for the four faults of ADR 0019; measured CI cost in ADR 0019
- [x] README: roadmap, events section, decision table rows 0016 to 0019; spec status done
