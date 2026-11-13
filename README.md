# double-entry-ledger

[![CI](https://github.com/LucasSantana-Dev/double-entry-ledger/actions/workflows/ci.yml/badge.svg)](https://github.com/LucasSantana-Dev/double-entry-ledger/actions/workflows/ci.yml)

A money transfer service whose central guarantee is that money is never created, lost or duplicated, even with thousands of concurrent requests and clients retrying. The guarantee is proven by a test that hammers a real PostgreSQL and then audits the books, not by a claim in this README.

Java 25 · Spring Boot 4.1 · Spring Data JPA · PostgreSQL 18 · Flyway · Testcontainers · virtual threads

## What the tests prove

`ConcurrentTransfersInvariantTest` fires a seeded random workload at the service on virtual threads: transfers in both directions between the same accounts, 30% of them touching one hot account, amounts large enough that some must fail, and 10% of the requests sent twice at the same time with the same idempotency key. Then it checks, straight in the database:

1. The sum of all balances is zero (money in = money out of the settlement account).
2. No customer balance is negative.
3. Every balance equals the sum of its entries.
4. Every transfer is exactly two entries that cancel out.
5. No idempotency key produced more than one transfer, and every replay returned the original one.
6. Every transfer has its idempotency key stored.
7. Entries cannot be updated or deleted (the database rejects it).

It also fails on any unexpected exception, so a single deadlock or lock timeout breaks the build. On top of the database checks, it keeps a client-side model of every transfer reported as created and asserts that the stored transfers and every final balance match it exactly.

Runs on 2026-11-13 (JDK 25, PostgreSQL 18 in Docker, Windows 11 laptop):

| Seed | Submissions | Created | Replayed | Insufficient funds | 409 then retried | Violations |
|---|---|---|---|---|---|---|
| 20261113 (default) | 5,533 over 5,000 keys | 3,815 | 398 | 1,320 | 522 | 0 |
| 7 | 21,975 over 20,000 keys | 14,878 | 1,433 | 5,664 | 1,881 | 0 |

The seed fixes the workload, not the thread interleaving, so the counts move a little between runs of the same seed. The invariants must hold on every run. Timings are not reported here: this test is built for contention, not speed, and throughput is measured properly in phase 2.

## Design decisions

Each decision has an ADR with the options, the evidence and the consequences.

| Decision | Choice | ADR |
|---|---|---|
| Runtime | Java 25 + Spring Boot 4.1 | [0001](docs/adr/0001-java-25-and-spring-boot-4-1.md) |
| Build | Maven | [0002](docs/adr/0002-maven.md) |
| Persistence | Spring Data JPA | [0003](docs/adr/0003-spring-data-jpa.md) |
| Money | `long` cents, Pix decimal string at the API | [0004](docs/adr/0004-money-as-long-cents.md) |
| Ledger model | Immutable entries + materialized balance | [0005](docs/adr/0005-entries-plus-materialized-balance.md) |
| Idempotency | PostgreSQL, same transaction, 409 while in flight | [0006](docs/adr/0006-idempotency-key-in-postgres.md) |
| Concurrency | `SELECT ... FOR UPDATE`, locks taken in id order | [0007](docs/adr/0007-pessimistic-locking-ordered-by-id.md) |
| Money entering | Settlement account + deposits | [0008](docs/adr/0008-settlement-account-and-deposits.md) |
| Property tests | Seeded randomized JUnit tests | [0009](docs/adr/0009-seeded-randomized-tests-instead-of-jqwik.md) |

Market evidence behind the stack choices: [126 Brazilian Java job postings](docs/market/2026-11-13-gupy-java-sample.md), reproducible with `tools/market/gupy_java_sample.py`.

## API

Amounts use the API Pix format, `"500.00"`. Every write needs an `Idempotency-Key` header. Errors are RFC 9457 problem details.

```bash
curl -s -X POST localhost:8080/accounts
curl -s -X POST localhost:8080/deposits -H 'Idempotency-Key: 7f9c...' -H 'Content-Type: application/json' \
  -d '{"accountId":"<id>","amount":"100.00"}'
curl -s -X POST localhost:8080/transfers -H 'Idempotency-Key: 1a2b...' -H 'Content-Type: application/json' \
  -d '{"fromAccountId":"<a>","toAccountId":"<b>","amount":"30.00"}'
curl -s localhost:8080/accounts/<id>
```

| Status | When |
|---|---|
| 201 | Created. A retry of a committed key returns the same body plus `Idempotent-Replayed: true` |
| 400 | Amount outside the Pix pattern, missing or invalid key, malformed body |
| 404 | Unknown account |
| 409 | Same key while the first request is still running: retry later |
| 422 | Insufficient funds, same key with a different payload, invalid transfer |
| 503 | Transfer kept conflicting with concurrent ones after 5 attempts; nothing was written, retry with the same key after `Retry-After` ([ADR 0010](docs/adr/0010-four-concurrency-strategies-and-bounded-retry.md)) |

## Running

Needs JDK 25 and Docker.

```bash
./mvnw verify                  # build and all tests
./mvnw spring-boot:test-run    # API on :8080 with a throwaway PostgreSQL container
```

Bigger or different invariant runs:

```bash
./mvnw test -Dtest=ConcurrentTransfersInvariantTest -Dledger.invariant.operations=20000 -Dledger.invariant.seed=7
```

## Roadmap

| Phase | Scope | Status |
|---|---|---|
| 1 | Accounts, deposits, transfers, idempotency, invariant test | done |
| 2 | Benchmark four locking strategies and virtual vs platform threads under load | planned |
| 3 | Transactional outbox to Kafka, idempotent consumer, reconciliation, fault injection | planned |
| 4 | Traces and metrics, deployment, results write-up | planned |
