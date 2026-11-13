# Phase 2 tasks

- [x] Spec and ADRs 0010 to 0013 from the decisions taken on 2026-11-13
- [x] Gupy sample rerun with load testing and concurrency terms
- [x] `ConcurrencyStrategy` interface; `pessimistic` extracted from `TransferService`
- [x] Invariant test parameterized by strategy (one `@Nested` class per strategy, own context and database)
- [x] Bounded retry and 503 `concurrency-conflict`; test client treats 503 like 409; retry counters in Micrometer
- [x] `conditional-update` strategy (mutation check: without id order the invariant run fails with 724 conflict retries)
- [x] V2 migration with `accounts.version`; `optimistic` strategy (updates flushed in id order via `hibernate.order_updates`)
- [ ] `serializable` strategy
- [ ] Spike: jmeter-java-dsl on Java 25 (at most one hour; Gatling fallback per ADR 0011)
- [ ] Load plan and one local cell under `-Pbenchmark`, metrics collected
- [ ] Benchmark workflow (manual, 24 cells x 5, shuffled) and results aggregation
- [ ] One full published run, results committed
- [ ] README results and limits; ADR 0007 points to them
