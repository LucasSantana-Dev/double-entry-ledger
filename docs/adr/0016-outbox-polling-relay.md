# 0016. Transactional outbox with a polling relay

- Status: accepted
- Date: 2026-11-16

## Context

Phase 3 publishes every committed transfer to Kafka. Writing to PostgreSQL and to Kafka are two commits that cannot be made atomic, so a crash between them either loses the event (database first) or publishes an event for a transfer that rolled back (Kafka first). The transactional outbox removes the dual write: the event is a row written in the same transaction as the transfer, and a separate process moves rows to Kafka.

Deposits and transfers share one code path (`TransferService.move`, ADR 0008: a deposit is a transfer from the settlement account), so one insert covers both.

## Options

1. **Polling publisher.** An `outbox_events` table. A `@Scheduled` relay claims unpublished rows with `SELECT ... FOR UPDATE SKIP LOCKED ORDER BY id LIMIT n`, sends them with `KafkaTemplate.send(...)`, waits for each acknowledgement with `.get(timeout)`, and marks them published in the same database transaction.
2. **Debezium Kafka Connect 3.7.1 with the Outbox Event Router.** Log-based change data capture reads the outbox inserts from the write-ahead log: latency in milliseconds, events keyed by aggregate id, no polling load.
3. **Embedded Debezium engine** inside the application: CDC without a Connect cluster.
4. **Spring Modulith 2.1 event publication registry.**
5. **PostgreSQL `LISTEN/NOTIFY`** to push events instead of polling.

## Evidence

- Market (Gupy sample of 100 Java postings, 2026-11-16, [docs/market/2026-11-16-gupy-phase-3-keywords.md](../market/2026-11-16-gupy-phase-3-keywords.md)): Kafka 36%, outbox 2%, CDC 2%, Debezium 0%, Spring Modulith 0%. The demand is Kafka itself; nobody asks for a specific outbox mechanism, so the one that shows the mechanics wins.
- Debezium Outbox Event Router ([docs](https://debezium.io/documentation/reference/stable/transformations/outbox-event-router.html), releases at [debezium.io/releases](https://debezium.io/releases/)): needs `wal_level=logical`, a replication slot and a Kafka Connect worker. A slot whose connector stops keeps the server from recycling WAL, so a stopped connector fills the disk of the ledger database. One more container and one more failure mode in CI.
- Embedded engine ([docs](https://debezium.io/documentation/reference/stable/development/engine.html)): after a crash it replays records already handled since the last offset flush (duplicates), and a handler that throws can make the engine move past the record. Neither is acceptable without extra machinery that the polling relay does not need.
- Spring Modulith ([docs](https://docs.spring.io/spring-modulith/reference/events.html)): the default registry records publications for in-process listeners and republishes incomplete ones; its own docs say this is not a full outbox for external brokers. The outbox mode through the Namastack integration hides the claim, send and mark steps that this project wants to show and test.
- PostgreSQL 18 `SELECT` ([docs](https://www.postgresql.org/docs/18/sql-select.html)): `SKIP LOCKED` skips rows locked by other transactions, the documented building block for queue-like tables. `NOTIFY` ([docs](https://www.postgresql.org/docs/18/sql-notify.html)) is delivered only to sessions listening at the time and is not durable, so it can only be a hint to poll sooner, never the source of truth.
- Gunnar Morling, [Revisiting the outbox pattern](https://www.morling.dev/blog/revisiting-the-outbox-pattern/): polling is simpler to run, log-based CDC has lower latency and less load on the database; both read the same table shape.

## Decision

Option 1, the polling relay.

- `outbox_events` (Flyway V3): `id bigint generated always as identity` (the ledger sequence), `event_id uuid unique`, `aggregate_type`, `aggregate_id`, `event_type`, `schema_version`, `payload jsonb`, `occurred_at`, `published_at`, `attempts`, `last_error`, `parked_at`. The same columns Debezium's router expects (aggregate id, type, payload), so the scale-out path reuses the table.
- The insert is the last statement of the transfer transaction. It runs after the account rows are locked or read, so the identity value is drawn after every earlier transfer on the same account committed: per account, `id` order is commit order. A rolled back transfer rolls its event back with it.
- The relay runs every 100 ms (`ledger.outbox.relay.interval-ms`). Each batch is one transaction that first takes `pg_try_advisory_xact_lock(4242, 1)`. Only one relay works at a time, across instances; another one skips the tick. The lock is transaction scoped, so a crashed relay never holds it.
- Kafka record key: the transfer id (the aggregate id). A transfer touches two accounts, so no account id can order both sides: keyed by source account, credits would arrive in any order, and every deposit (source: the settlement account) would land on one partition. Per-account order is carried by the ledger sequence in the event envelope instead (ADR 0018), and the statement orders lines by it (ADR 0017). The transfer id spreads load across partitions and is the key Debezium's router would use.
- The relay sends the whole batch, then waits for each acknowledgement in id order (`.get(timeout)`). Rows acknowledged are marked `published_at`. On the first retriable failure (broker down, timeout) the rest of the batch stays unpublished and the next tick retries it. Producer: `enable.idempotence=true`, `acks=all`.
- Delivery is at least once. A crash or a lost database connection after the send and before the mark resends the batch; the consumer deduplicates (ADR 0017).
- Poison messages: a send that fails with a non-retriable Kafka error (for example `RecordTooLargeException`) parks the row (`parked_at`, `last_error`) and the relay moves on. Parked rows are never deleted; reconciliation reports their transfers as missing from the statement until someone fixes and unparks them, and the statement still slots them in the right place because it orders by sequence.
- Cleanup: an hourly job deletes rows published more than 7 days ago (`ledger.outbox.retention`). The "one event per transfer" check therefore holds within the retention window.

## Consequences

- No extra infrastructure: PostgreSQL and Kafka only.
- Latency between commit and publish is up to one polling interval plus the send, against milliseconds for log-based CDC. `NOTIFY` after commit can later wake the relay early without changing correctness.
- Single active relay: throughput of publishing is bounded by one batch at a time. That is the price of keeping one global send order; with `SKIP LOCKED` already in the claim query, running several relays is a configuration change that gives up cross-batch order, which the statement tolerates because it orders by sequence.
- A batch holds a database transaction open while it waits for Kafka, at most the send timeout (10 s). With the broker down, every tick waits that long.
- Scale-out path: Debezium Connect with the Outbox Event Router on the same `outbox_events` table, once latency or polling load matters, accepting the replication slot to operate.
