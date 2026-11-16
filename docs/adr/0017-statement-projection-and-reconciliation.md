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
- The listener's error handler sorts failures by cause. When the database is unreachable (Spring's transient, recoverable or resource-failure exceptions, a transaction that cannot start, JDBC `SQLTransientException`/`SQLRecoverableException`, SQLSTATE class `08`) it retries every second without a limit, so an outage pauses consumption and never dead-letters an event; `FaultInjectionTest` asserts zero dead letters after the database cut and the consumer crash. Any other failure is a poison pill (an event that deserializes but cannot be applied, such as one without account ids): three more tries 500 ms apart, then `DeadLetterPublishingRecoverer` sends it to `ledger.transfers.DLT`, same partition, with the exception in the headers, and the partition moves on. Deserialization failures go to the dead letter topic without retries. The counter `ledger.statement.dead-lettered` counts them, and reconciliation reports their transfers as missing.
- Dead letter topic rather than a `statement.parked_events` table: the record keeps its original bytes, key, partition and offset plus the failure headers, which is what a replay needs, and it does not depend on the database being writable, which is one of the ways a record can fail. The topic is named explicitly because Spring for Apache Kafka 4 defaults to a `-dlt` suffix.
- Offsets are committed by the listener container after the record is processed; a crash after the database commit and before the offset commit redelivers the record, and the dedup table absorbs it.
- Reconciliation: option 1, `@Scheduled` every minute (`ledger.reconciliation.interval-ms`), over a sliding window of the last 24 hours (`ledger.reconciliation.window-hours`) ending at the maximum relay delay (`ledger.reconciliation.lag-ms`, 60 s), so a run costs the same however long the ledger grows. `reconcileAll` checks the whole history for tests and manual use. Transfers whose outbox row is still unpublished are left out of every comparison, on both sides, and listed apart: pending (on their way, not a difference, so a broker outage longer than the lag does not raise false alarms) or parked (rejected by Kafka for good, a difference). It reports:
  - transfers missing from the statement, transfers with other than two lines, and lines whose transfer does not exist;
  - per hour window: transfer count and credited sum, ledger against statement;
  - for windows that differ, the per-account sums that differ.
  It logs one line (`reconciliation clean` or the differences at `WARN`) and returns a `ReconciliationReport` that tests assert on.

## Consequences

- One deployable and one database; the consumer can be split out (option 2) without changing its logic, only its datasource.
- `processed_events` grows with every event; a retention job is a later concern, safe once it is longer than the topic retention.
- The scheduled reconciliation reads 24 hours per run; an index on `transfers.created_at` and `entries.created_at` becomes worth it when that window holds millions of rows. A difference older than the window is only found by `reconcileAll`.
- A dead-lettered event needs a person: fix the cause, then replay the record from `ledger.transfers.DLT` (deduplication makes a replay safe). There is no automatic replay.
- Several instances would run reconciliation once each. Harmless (read-only) but noisy; ShedLock 7.11 is the multi-instance path.
