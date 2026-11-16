---
status: in-progress
date: 2026-11-16
phase: 3
tags: [outbox, kafka, idempotency, reconciliation, fault-injection]
---

# Phase 3: transactional outbox, idempotent consumer, reconciliation, fault injection

## Goal

Every committed transfer becomes exactly one event on Kafka, and a consumer turns those events into an account statement that matches the ledger, even when the broker goes down, the database connection drops mid-publish, the consumer crashes between its commit and its offset commit, and messages arrive twice. As in phase 1, the guarantees are proven by tests against real infrastructure, not asserted.

## Scope

1. Outbox (ADR 0016): table `outbox_events` (Flyway V3), one row per transfer and per deposit, written as the last statement of the transfer transaction.
2. Polling relay (ADR 0016): `@Scheduled` every 100 ms, one batch per transaction, `pg_try_advisory_xact_lock` so a single relay works at a time, `SELECT ... FOR UPDATE SKIP LOCKED ORDER BY id LIMIT 500`, send through `KafkaTemplate`, wait for each acknowledgement, mark published. Idempotent producer, `acks=all`. Topic `ledger.transfers`, 3 partitions, key the transfer id.
3. Poison messages: a non-retriable send failure parks the row (`parked_at`, `last_error`); the relay continues with the next rows.
4. Outbox cleanup: an hourly job deletes rows published more than 7 days ago; parked and unpublished rows are kept.
5. Event format (ADR 0018): JSON envelope `eventId`, `eventType`, `schemaVersion`, `occurredAt`, `aggregateType`, `aggregateId`, `sequence`, `payload` (`transferId`, `fromAccountId`, `toAccountId`, `amountCents`), through Spring for Apache Kafka's Jackson 3 serializers.
6. Statement projection (ADR 0017): schema `statement` (Flyway V4) with `processed_events` (event id primary key) and `statement_lines` (one line per account per event), written by a `@KafkaListener` in one transaction. Duplicates are detected by the event id and skipped. Lines are ordered by `sequence`.
7. Reconciliation (ADR 0017): scheduled every minute over transfers older than 60 s; reports transfers missing from the statement, transfers with other than two lines, lines without a transfer, per-hour count and credited-sum differences, and per-account differences inside differing windows. Logs one line and returns a `ReconciliationReport`.
8. Fault injection (ADR 0019): Toxiproxy between the application and PostgreSQL, Kafka paused through the Docker API, a hook that crashes the consumer after its database commit, a forced re-send of published events.

Out of scope: a statement HTTP endpoint, a separate consumer service or database, Debezium, Kafka transactions, Avro or Protobuf, ShedLock and several application instances, retention of `processed_events`, metrics and traces (phase 4).

## Invariants (the acceptance criteria)

1. Every committed transfer (deposits included) has exactly one outbox event, written in the same transaction; a rolled back transfer has none. Checked within the outbox retention window.
2. Every outbox event is eventually published: none is left unpublished after the relay drains, including after the broker was down and after the database connection was cut between the send and the mark-published update.
3. The statement applies each event exactly once, even with duplicate deliveries and a consumer crash before the offset commit.
4. After all fault scenarios, reconciliation reports zero differences and every account's statement balance equals its ledger balance.
5. Per-account event order in the statement matches commit order for that account (within the single-relay limit of ADR 0016).

Plus the seven phase 1 invariants, unchanged, for all four concurrency strategies.

## How the tests check them

| Invariant | Test |
|---|---|
| 1 | `ConcurrentTransfersInvariantTest`, all four strategies: outbox rows equal transfers, each transfer has exactly one row, in a workload where many attempts roll back (insufficient funds, conflicts, retries) |
| 2 | `ConcurrentTransfersInvariantTest` (drain after the workload), `OutboxRelayTest`, `FaultInjectionTest` |
| 3 | `StatementProjectionTest` (re-sent and crashed deliveries), `FaultInjectionTest` |
| 4 | `ConcurrentTransfersInvariantTest`, `ReconciliationTest`, `FaultInjectionTest` |
| 5 | `ConcurrentTransfersInvariantTest`: per account, lines in sequence order have non-decreasing transfer timestamps and a running balance that never goes below zero for customers; `StatementProjectionTest`: out-of-order delivery still yields sequence order |

## Decisions

| ADR | Decision |
|---|---|
| 0016 | Outbox table with a polling relay, single active relay by advisory lock, key the transfer id, at-least-once |
| 0017 | Statement projection in the same application, dedup table in the same transaction, scheduled reconciliation |
| 0018 | JSON envelope through Jackson 3, amounts in `long` cents |
| 0019 | Toxiproxy for PostgreSQL, Docker pause for Kafka, test hooks for the consumer crash, forced re-send for duplicates |

## Done when

- `./mvnw verify` passes with the five phase 3 invariants and the seven phase 1 invariants green for all four strategies.
- ADR 0019 records the measured CI cost of phase 3.
- README states the phase 3 invariants and how the faults are injected.
