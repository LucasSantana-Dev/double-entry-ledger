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

Load benchmark (Docker; one cell, short windows):

```bash
./mvnw -Pbenchmark verify -Dledger.bench.cells=strategy-pessimistic-hot30 -Dledger.bench.warmup=PT5S -Dledger.bench.measure=PT10S
```

## Benchmark results

One run on a GitHub-hosted runner (4 vCPU AMD EPYC 9V45, Java 25), 24 cells × 5 repetitions in shuffled order, 30 s warm-up and 60 s measured per cell, a fresh PostgreSQL 18 and a fresh application JVM per cell ([ADRs 0012 to 0015](docs/adr/0012-benchmark-scenarios-and-metrics.md)). Values are medians over the 5 repetitions. Full tables with 95% bootstrap intervals: [results.md](docs/benchmarks/2026-11-13/results.md); raw rows: [results.jsonl](docs/benchmarks/2026-11-13/results.jsonl). After every cell the ledger checks passed (entries sum to zero, no negative customer balance, balances match entries).

**Strategies** (256 users, virtual threads; "hot" is the share of transfers that touch one shared account):

| strategy | created/s 0% / 30% / 90% hot | p99 ms (201) 0% / 30% / 90% | 503 per minute at 90% hot |
|---|---|---|---|
| conditional-update | 2,042 / 1,602 / 983 | 354 / 359 / 337 | 0 |
| pessimistic | 1,878 / 1,458 / 768 | 380 / 395 / 416 | 0 |
| optimistic | 1,813 / 1,043 / 292 | 391 / 676 / 745 | 16,158 |
| serializable | 1,557 / 808 / 228 | 446 / 830 / 917 | 13,186 |

- The two strategies that wait instead of aborting (conditional-update, pessimistic) degrade gracefully as contention grows and never return 503. Conditional-update is ahead of pessimistic in every cell: one `UPDATE ... WHERE balance >= x` per account instead of a `SELECT ... FOR UPDATE` plus an `UPDATE`.
- The two that abort (optimistic, serializable) collapse under a hot account: at 90% they create 3 to 4 times fewer transfers than conditional-update, and a large share of requests end in 503 after 5 attempts. Without a hot account optimistic is close to pessimistic, and serializable creates 17 to 24% fewer transfers than the waiting strategies.

**Virtual vs platform threads** (pessimistic strategy):

| users | hot | created/s platform / virtual | p99 ms platform / virtual | platform threads platform / virtual |
|---|---|---|---|---|
| 64 | 0% | 1,934 / 1,967 | 90 / 81 | 87 / 22 |
| 64 | 90% | 874 / 764 | 130 / 127 | 87 / 22 |
| 1,024 | 0% | 1,776 / 1,906 | 1,011 / 1,550 | 215 / 22 |
| 1,024 | 90% | 927 / 736 | 1,498 / 1,600 | 215 / 22 |

- Virtual threads do not raise throughput: 10 database connections cap the work in flight, so more request threads only add queueing. That was the hypothesis of ADR 0012 and the data agrees. Most cells are within about 10% with overlapping intervals; at 1,024 users and 90% hot, platform threads are ahead (927 against 736 created/s, intervals apart).
- Virtual threads keep the platform thread count flat (22 against 215 at 1,024 users). At 1,024 users they cost about 170 MB more heap (about 835 against 665 MB) and a longer p99 tail. A likely reason, not measured here: with virtual threads every request is admitted and its object graph lives while it waits for a connection, while Tomcat's 200-thread pool keeps the excess in the accept queue.

**Limits of these numbers.** One run on a shared cloud runner, with the load generator, the application and PostgreSQL on the same 4 vCPU VM; absolute numbers will differ elsewhere, the comparison between cells is the result. 5 repetitions make the bootstrap intervals coarse. PostgreSQL's CPU and the time spent waiting for a Hikari connection are not measured (ADR 0012); the application used about one core of four and the load generator under a third of a core, so the database and the 10-connection pool are the likely limit. Each cell starts from fresh tables with 1,000 accounts, so table and index bloat over hours is not covered.

## Roadmap

| Phase | Scope | Status |
|---|---|---|
| 1 | Accounts, deposits, transfers, idempotency, invariant test | done |
| 2 | Benchmark four locking strategies and virtual vs platform threads under load | done |
| 3 | Transactional outbox to Kafka, idempotent consumer, reconciliation, fault injection | planned |
| 4 | Traces and metrics, deployment, results write-up | planned |
