# 0010. Four concurrency strategies under test, bounded retry with 503

- Status: accepted
- Date: 2026-11-13

## Context

ADR 0007 picked pessimistic row locks for phase 1 and promised that phase 2 would measure the alternatives it listed. Two of them (optimistic and `SERIALIZABLE`) abort a transaction on conflict, so the service needs a rule for what happens after an abort. Today the invariant test fails on any exception other than 409 (key in progress) and insufficient funds.

## Options

Strategies:

1. The four options of ADR 0007: (a) `SELECT ... FOR UPDATE` in id order, (b) optimistic with a `version` column and retry, (c) `SERIALIZABLE` isolation with retry on SQLSTATE `40001`, (d) a single conditional `UPDATE ... SET balance_cents = balance_cents - x WHERE id = ? AND balance_cents >= x` checking the row count.
2. Replace (c) with transaction-scoped advisory locks per account. Already used for idempotency (ADR 0006), but it is a second pessimistic variant, so the comparison loses the isolation-level family.
3. Replace (b) with an in-JVM striped `ReentrantLock` per account. Likely fastest on one instance, but wrong with two instances, which phase 4 deploys.

Retry after a conflict abort:

1. Up to 5 attempts in the service with exponential backoff and jitter (about 1, 2, 4, 8 ms). When they run out, answer 503 with problem type `concurrency-conflict` and `Retry-After`; clients retry with the same idempotency key, as they do for 409.
2. Retry without a limit. No API change, but latency has no ceiling under a hot account and p99 measures the loop, not the strategy.
3. No server retry. Every abort reaches the client. Purest measurement, but pushes the work onto clients we do not own.

## Evidence

- ADR 0007 already committed to these four options, and they cover the three classic families (pessimistic lock, optimistic validation, database isolation) plus write-without-read.
- PostgreSQL 18 documentation, section 13.2.3: applications using `SERIALIZABLE` must be prepared to retry transactions that fail with serialization errors. The optimistic strategy has the same need on a version mismatch.
- Spring Retry and Resilience4j both default to bounded attempts with backoff, the common shape in Java services.

## Decision

Strategies: option 1, all four from ADR 0007. Retry: option 1, five attempts with backoff, then 503 `concurrency-conflict`.

The strategy is selected by configuration, so the same build runs any of them and the invariant test runs once per strategy.

## Consequences

- A new migration adds `accounts.version` for strategy (b). The other strategies ignore it.
- The API gains a 503 response. The phase 2 spec records it; the invariant test's client treats it like 409.
- Retry count and 503 rate become benchmark metrics next to throughput and latency.
- Every strategy must pass `ConcurrentTransfersInvariantTest` unchanged in its assertions. A strategy that breaks an invariant is a bug, not a result.
