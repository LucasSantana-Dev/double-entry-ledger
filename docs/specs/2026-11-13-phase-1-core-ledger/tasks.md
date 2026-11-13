# Phase 1 tasks

- [x] Scaffold from Spring Initializr (Boot 4.1.2, Java 25, Maven)
- [x] ADRs 0001 to 0009 from the decisions taken on 2026-11-13
- [x] Schema migration: accounts, transfers, entries, idempotency keys, append-only trigger, settlement account
- [x] `Money`: Pix decimal string to cents and back
- [x] Account, Transfer and Entry entities with repositories
- [x] `TransferService`: advisory lock on the key, replay, ordered pessimistic locks, debit and credit, entries
- [x] REST controllers and problem details
- [x] API tests (MockMvc + Testcontainers)
- [x] Concurrent invariant test (5,000 operations, virtual threads, duplicates, hot account)
- [x] `./mvnw verify` green (32 tests; invariant run also green at 20,000 operations, seed 7)
- [x] README with results
