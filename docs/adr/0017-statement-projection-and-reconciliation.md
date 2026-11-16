# 0017. Statement projection with an idempotent consumer, and reconciliation

- Status: accepted
- Date: 2026-11-16

## Context

Publishing events (ADR 0016) proves nothing without a consumer whose result can be checked. The relay delivers at least once, so the consumer must apply each event exactly once by itself. A projection that silently drifts from the ledger is worse than none, so something must compare the two.

## Options

Where the consumer lives:

1. An account statement projection (read model) in a separate schema, `statement`, of the same application and database, fed by a `@KafkaListener`.
2. A separate service with its own database. Closer to production topology, but doubles the configuration, the containers and the CI time for the same lesson.

How it deduplicates:

1. A `statement.processed_events` table with the event id as primary key, inserted in the same database transaction as the statement lines. A duplicate key means the event was already applied; the transaction does nothing else.
2. Natural idempotency by upsert. Works for "set balance to X" projections, breaks for an append-only statement where the same line twice is a real error.
3. Kafka exactly-once (transactions plus `read_committed`).

Reconciliation scheduling:

1. A plain `@Scheduled` job in the single instance.
2. ShedLock 7.11 to run it on one of several instances.

## Evidence

- Spring for Apache Kafka, [transactions](https://docs.spring.io/spring-kafka/reference/kafka/transactions.html): Kafka transactions make consume, process and produce atomic within Kafka. A database write next to them is committed sequentially (best effort), not atomically, so exactly-once into PostgreSQL still needs consumer-side deduplication. Option 3 adds cost without removing option 1.
- Market (Gupy sample, 2026-11-16, [docs/market/2026-11-16-gupy-phase-3-keywords.md](../market/2026-11-16-gupy-phase-3-keywords.md)): idempotency 7%, event-driven 17%, saga 2%, ShedLock 0%. Idempotent consumers are the part of event-driven work that postings name.
- The ledger and the statement share one PostgreSQL in this project, but the consumer touches only the `statement` schema and talks to the ledger through Kafka alone, so splitting it out later moves code, not logic.

## Decision

- Consumer: option 1, the statement projection in schema `statement` (Flyway V4).
- Deduplication: option 1. `statement_lines` holds one line per account per event (the debit and the credit), with the event id, transfer id, ledger sequence, amount and `occurred_at`. `processed_events` and the lines are written in one transaction; `insert ... on conflict do nothing` returning zero rows marks a duplicate.
- The statement is ordered by the ledger sequence, not by arrival, so redeliveries or cross-partition interleaving cannot reorder an account's lines (ADR 0016).
- The listener's error handler retries without a limit (fixed 500 ms back-off) so a database outage pauses consumption instead of skipping records. Deserialization failures are not retried; they are logged and skipped, and reconciliation reports the missing transfer.
- Offsets are committed by the listener container after the record is processed; a crash after the database commit and before the offset commit redelivers the record, and the dedup table absorbs it.
- Reconciliation: option 1, `@Scheduled` every minute (`ledger.reconciliation.interval-ms`), over transfers older than the maximum relay delay (`ledger.reconciliation.lag-ms`, 60 s). It reports:
  - transfers missing from the statement, transfers with other than two lines, and lines whose transfer does not exist;
  - per hour window: transfer count and credited sum, ledger against statement;
  - for windows that differ, the per-account sums that differ.
  It logs one line (`reconciliation clean` or the differences at `WARN`) and returns a `ReconciliationReport` that tests assert on.

## Consequences

- One deployable and one database; the consumer can be split out (option 2) without changing its logic, only its datasource.
- `processed_events` grows with every event; a retention job is a later concern, safe once it is longer than the topic retention.
- Reconciliation reads the whole history window by window; fine for this data size, an index on `transfers.created_at` or incremental checkpoints when it is not.
- Several instances would run reconciliation once each. Harmless (read-only) but noisy; ShedLock 7.11 is the multi-instance path.
