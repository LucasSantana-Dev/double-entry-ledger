# Keeping a ledger consistent under concurrency and failure, with tests as the evidence

> Draft, not published.

A money transfer service has one job that matters more than the rest: money is never created, lost or counted twice. Most of the ways it breaks involve more than one request. Two requests debit the same account at the same moment. A client times out and sends the same transfer again. The process commits to the database and dies before telling Kafka. Kafka delivers an event twice. A test suite that runs one request at a time on the happy path sees none of them.

This post walks through a small Java project, [double-entry-ledger](../../README.md), built in four phases. Each phase states a property, and each property has a test that runs against real PostgreSQL and real Kafka in containers. Every number below comes from the repository's docs and benchmark data.

## The failure modes

Three classes of bug drive the design.

**Lost updates and double spends.** Two transfers read a balance of 100.00, each subtracts 80.00, each writes 20.00. One debit vanishes. Any read, compute, write sequence on a balance has this race unless something serializes the writers.

**Duplicate requests.** A client retries after a lost response. Without idempotency the retry is a second transfer; with a naive check (look up the key, then insert) two concurrent retries both see "not found" and both insert.

**Lost or duplicated events.** Writing to PostgreSQL and publishing to Kafka are two commits. Commit the database first and crash, and the event is lost; publish first and roll back, and consumers see a transfer that never happened. Even with that solved, delivery is at least once, so consumers see some events twice.

## Phase 1: the ledger and the invariant test

The model is double entry ([ADR 0005](../adr/0005-entries-plus-materialized-balance.md)). Every transfer writes two immutable entries that cancel out, and each account keeps a materialized balance updated in the same transaction. Money enters through a settlement account, so a deposit is a transfer from it ([ADR 0008](../adr/0008-settlement-account-and-deposits.md)) and the sum of all balances is always zero. Amounts are `long` cents internally ([ADR 0004](../adr/0004-money-as-long-cents.md)).

Idempotency keys live in PostgreSQL, written in the same transaction as the transfer ([ADR 0006](../adr/0006-idempotency-key-in-postgres.md)). A unique constraint settles the race: while the first request runs, a concurrent retry gets a 409; after it commits, a retry gets the original response.

The test that carries the weight is `ConcurrentTransfersInvariantTest`. It fires a seeded random workload on virtual threads: transfers in both directions between the same accounts, 30% of them touching one hot account, amounts large enough that some must fail for insufficient funds, and 10% of requests sent twice at the same time with the same idempotency key. Then it audits the database directly for seven invariants: balances sum to zero, no customer balance is negative, every balance equals the sum of its entries, every transfer is exactly two cancelling entries, no key produced more than one transfer, every transfer has its key stored, and entries cannot be updated or deleted. Any unexpected exception fails the run, so a single deadlock breaks the build. A client-side model of every transfer reported as created must also match the stored transfers and every final balance.

Two recorded runs on 2026-11-13 ([README](../../README.md#what-the-tests-prove)):

| Seed | Submissions | Created | Replayed | Insufficient funds | 409 then retried | Violations |
|---|---|---|---|---|---|---|
| 20261113 | 5,533 over 5,000 keys | 3,815 | 398 | 1,320 | 522 | 0 |
| 7 | 21,975 over 20,000 keys | 14,878 | 1,433 | 5,664 | 1,881 | 0 |

The seed fixes the workload. It cannot fix the thread interleaving, so counts move a little between runs, and the invariants have to hold on every one of them. Seed and size are system properties, so a failing run reproduces with the same workload ([ADR 0009](../adr/0009-seeded-randomized-tests-instead-of-jqwik.md)).

## Phase 2: four strategies, measured

Phase 1 used `SELECT ... FOR UPDATE` with locks taken in account id order, which avoids deadlocks by construction ([ADR 0007](../adr/0007-pessimistic-locking-ordered-by-id.md)). It was the easiest version to prove correct. Phase 2 made the strategy selectable and benchmarked four of them ([ADR 0010](../adr/0010-four-concurrency-strategies-and-bounded-retry.md)):

- **pessimistic**: `SELECT ... FOR UPDATE` in id order, then `UPDATE`;
- **conditional update**: a single `UPDATE ... SET balance = balance - x WHERE id = ? AND balance >= x` per account, in id order, checking the row count;
- **optimistic**: a version column, retried on conflict;
- **serializable**: `SERIALIZABLE` isolation, retried on serialization failure.

The two that abort on conflict make at most 5 attempts with backoff and then answer 503 with nothing written, so the client can retry with the same key. All four pass the invariant test, so correctness does not separate them. Throughput and tail latency do.

The benchmark ran on a GitHub-hosted runner (4 vCPU AMD EPYC 9V45, Java 25): 24 cells, 5 repetitions each in shuffled order, 30 s warm-up and 60 s measured, a fresh PostgreSQL 18 and a fresh application JVM per cell ([ADRs 0012 to 0015](../adr/0012-benchmark-scenarios-and-metrics.md)). Values are medians; "hot" is the share of transfers touching one shared account. Intervals are in [results.md](../benchmarks/2026-11-13/results.md).

| strategy | created/s 0% / 30% / 90% hot | p99 ms 0% / 30% / 90% | 503 per minute at 90% hot |
|---|---|---|---|
| conditional-update | 2,042 / 1,602 / 983 | 354 / 359 / 337 | 0 |
| pessimistic | 1,878 / 1,458 / 768 | 380 / 395 / 416 | 0 |
| optimistic | 1,813 / 1,043 / 292 | 391 / 676 / 745 | 16,158 |
| serializable | 1,557 / 808 / 228 | 446 / 830 / 917 | 13,186 |

The strategies that wait on a row lock degrade gradually as contention grows and never returned a 503. The strategies that abort collapse under a hot account: at 90% hot they create 3 to 4 times fewer transfers than conditional update, and thousands of requests per minute end in 503. Conditional update beats pessimistic in every cell, by 9%, 10% and 28% at 0%, 30% and 90% hot, because it saves a round trip per account. It became the default ([ADR 0020](../adr/0020-default-strategy-conditional-update.md)). The cost is that the rule "the balance covers the debit" now lives in a SQL predicate instead of the entity.

The same benchmark compared virtual and platform threads. Virtual threads did not raise throughput: 10 database connections cap the work in flight, so more request threads only queue. They kept the platform thread count flat (22 against 215 at 1,024 users) and used about 170 MB more heap at that load. All of this is one run on a shared VM, so the comparison between cells is the result and the absolute values will differ elsewhere.

## Phase 3: events that survive failures

Every committed transfer becomes a `TransferCommitted` event on the Kafka topic `ledger.transfers`, and an account statement is built from those events in a separate schema. The dual write is removed with a transactional outbox ([ADR 0016](../adr/0016-outbox-polling-relay.md)). The event is a row in `outbox_events`, inserted as the last statement of the transfer transaction, so it commits or rolls back with the transfer. A relay polls every 100 ms, claims rows with `FOR UPDATE SKIP LOCKED`, sends them, waits for each acknowledgement and marks them published. A transaction-scoped PostgreSQL advisory lock keeps one relay active across instances. Polling won over Debezium because it needs no replication slot or Connect worker and keeps each step testable; Debezium remains the scale-out path on the same table.

Delivery is at least once, so the consumer deduplicates ([ADR 0017](../adr/0017-statement-projection-and-reconciliation.md)). It inserts the event id into `processed_events` in the same transaction as the statement lines; a conflict means the event was already applied and the transaction does nothing else. Kafka transactions would not remove this table, since the PostgreSQL write would still commit separately. Statement lines are ordered by the ledger sequence the event carries, so redeliveries and partition interleaving cannot reorder an account's history.

A reconciliation compares the ledger with the statement every minute; events still waiting in the outbox are reported apart, so a broker outage raises no false alarms.

`FaultInjectionTest` breaks things for real ([ADR 0019](../adr/0019-fault-injection.md)):

- **broker down**: the Kafka container is paused through the Docker API;
- **database connection cut between the send and the mark-published update**: PostgreSQL is reached only through Toxiproxy, and a hook disables the proxy right after Kafka acknowledged the batch, so the driver sees a real I/O failure;
- **consumer crash between its commit and the offset commit**: a hook throws after the statement transaction, so the record is redelivered;
- **duplicate delivery**: published rows are set back to pending and sent again.

After each fault the test asserts that every event was published, the statement applied each one exactly once, reconciliation reports zero differences, and statement balances equal ledger balances. The invariant test checks the same five event properties for all four strategies, including that each account's statement follows commit order, compared against the commit timestamps PostgreSQL records.

Phase 3 took `./mvnw verify` from 1 min 40 s to 3 min 27 s locally (55 to 69 tests), and the GitHub Actions job took 3 min 32 s. Most of the 50 s of fault scenarios is the broker pause waiting for the producer's delivery timeout.

## Phase 4: one trace through the outbox

Asynchronous systems usually lose the trace at the queue. Here the relay sends an event long after the request that wrote it has finished, on a scheduler thread. Phase 4 carries the context across that gap ([ADR 0021](../adr/0021-tracing-micrometer-otel-and-outbox-traceparent.md)): the transfer transaction stores the current span's W3C `traceparent` in the outbox row, and the relay opens its send span with that stored context as the parent. Kafka observation then puts a `traceparent` header on the record, and the consumer span becomes its child. A row without a context, for example one written while the request was not sampled, gets a new root span and is published anyway.

`TracingTest` captures spans in process from the real OpenTelemetry SDK that Spring Boot configures. One transfer must give one trace id with the HTTP server span as root, JDBC spans below it, the `outbox relay` span whose parent is the stored context, the Kafka producer span under it and the consumer span under that. It also checks the header on the actual record, and that an unsampled transfer or a row with a null `traceparent` is still published and applied.

Metrics cover what the stock meters do not ([ADR 0022](../adr/0022-otel-lgtm-stack-metrics-and-logs.md)): pending and parked outbox rows and the age of the oldest pending one, retries and 503s tagged by strategy and operation, the differences found by the last scheduled reconciliation and the time of its last success, and dead-lettered statement events. `ObservabilityMetricsTest` pauses Kafka and checks that the pending count and the oldest age rise and fall back after the drain, and that under a hot account the 503 counter equals the 503 responses. `OtlpExportSmokeTest` sends a transfer to a real `grafana/otel-lgtm` container and checks that its trace and a ledger metric arrive.

`docker compose up` starts the application, PostgreSQL, Kafka and Grafana with a provisioned dashboard. The [README](../../README.md#observability-and-deployment) shows it during a 150 s Kafka pause, with the outbox backlog climbing and draining, and a trace in Tempo where the wait in the outbox sits between the request and the send span.

The service also runs on Kubernetes ([ADR 0024](../adr/0024-kubernetes-on-kind-in-ci.md)): a multi-stage Dockerfile, Kustomize manifests with PostgreSQL and single-node KRaft Kafka as StatefulSets, and a GitHub Actions workflow that creates a kind cluster, deploys, waits for the rollouts and runs a smoke test that ends with the transfer's statement lines in the database.

## What the numbers say

- The invariants held on every recorded run, for all four strategies, including 21,975 submissions with duplicate keys sent concurrently. The test fails on any deadlock or lock timeout as well as on a broken balance.
- Under contention, how a strategy handles a conflict matters more than how it detects one. Waiting on a row lock kept throughput at 983 created transfers per second at 90% hot with conditional update; retrying after an abort fell to 292 and 228.
- With a 10-connection pool as the bottleneck, virtual threads kept the thread count flat and left throughput where it was.
- At-least-once delivery plus a dedup row in the consumer's own transaction gave exactly-once application under every injected fault, without Kafka transactions.

## Limits and what was left out

The benchmark is one run with 5 repetitions per cell, so the intervals are coarse. PostgreSQL's CPU and the wait for a pool connection were not measured, and fresh tables per cell leave bloat over hours uncovered.

There is one application instance and one active relay by design, so publishing is bounded by one batch at a time. The statement consumer shares the application and database with the ledger, though it touches only its own schema. Kafka and PostgreSQL are single nodes everywhere.

Deliberately out of scope: a public deployment or any cloud account, Terraform, Helm, the Strimzi operator, alerting rules, OTLP log export, the OpenTelemetry Java agent, and a production observability backend. `grafana/otel-lgtm` is a demo image and needs about 2 GB of memory. There is no automatic replay of dead-lettered events; that needs a person.

## Where to look

- [README](../../README.md): what is proven, by which test, with which numbers.
- [ADRs](../adr/): every design decision with options, evidence and consequences.
- Phase specs: [1](../specs/2026-11-13-phase-1-core-ledger/spec.md), [2](../specs/2026-11-13-phase-2-benchmarks/spec.md), [3](../specs/2026-11-16-phase-3-outbox/spec.md), [4](../specs/2026-11-16-phase-4-observability-deploy/spec.md).
- [Benchmark results](../benchmarks/2026-11-13/results.md) and the raw rows next to them.
