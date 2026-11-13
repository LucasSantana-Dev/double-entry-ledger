# Phase 2 tasks

- [x] Spec and ADRs 0010 to 0013 from the decisions taken on 2026-11-13
- [x] Gupy sample rerun with load testing and concurrency terms
- [x] `ConcurrencyStrategy` interface; `pessimistic` extracted from `TransferService`
- [x] Invariant test parameterized by strategy (one `@Nested` class per strategy, own context and database)
- [x] Bounded retry and 503 `concurrency-conflict`; test client treats 503 like 409; retry counters in Micrometer
- [x] `conditional-update` strategy (mutation check: without id order the invariant run fails with 724 conflict retries)
- [x] V2 migration with `accounts.version`; `optimistic` strategy (updates flushed in id order via `hibernate.order_updates`)
- [x] `serializable` strategy (`SerializableIsolationTest` proves the level; the invariant test alone would pass at read committed thanks to `@Version`)
- [x] Spike: jmeter-java-dsl on Java 25 works (result in ADR 0011); app must run in its own JVM per cell
- [x] Load plan under `-Pbenchmark` (`LedgerBenchmark`, ADR 0014): child JVM and fresh PostgreSQL per cell, metrics and ledger check per cell; smoke-run locally on 3 cells
- [ ] Benchmark workflow (manual, 24 cells x 5, shuffled) and results aggregation
- [ ] One full published run, results committed
- [ ] README results and limits; ADR 0007 points to them
