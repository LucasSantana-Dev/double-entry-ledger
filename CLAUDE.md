# double-entry-ledger

Portfolio project: a double-entry transfer ledger whose consistency under concurrency is proven by tests against real PostgreSQL. Java 25, Spring Boot 4.1, Maven wrapper, Spring Data JPA, Flyway, Testcontainers.

## Commands

- `./mvnw verify`: full build and tests (needs Docker running for Testcontainers).
- `./mvnw test -Dtest=ConcurrentTransfersInvariantTest -Dledger.invariant.operations=20000 -Dledger.invariant.seed=7`: bigger or different invariant run.
- `./mvnw spring-boot:test-run`: run the API locally on a throwaway PostgreSQL container.

## Rules

- Design decisions belong to the repo owner and are taken from data. Each one gets an ADR in `docs/adr/` with options, evidence (numbers with sources) and consequences. Trivial choices (names, file layout) do not need one.
- Work is organized in phases with a spec under `docs/specs/<date>-<slug>/`. Do not implement what the active spec does not cover; extend the spec first.
- The invariant test is the contract. A change that breaks any of the seven invariants in the phase 1 spec is a bug, whatever else it improves.
- Repo docs are in English. Never use the em dash or en dash.
- Commits carry no AI co-author trailer.
