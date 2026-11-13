# 0003. Spring Data JPA (Hibernate) for persistence

- Status: accepted
- Date: 2026-11-13

## Context

Phase 2 compares locking strategies, so the SQL that runs must be knowable. The project also exists to close a Java gap on a resume that already shows SQL and Prisma.

## Options

1. Spring Data JPA (Hibernate): ORM with explicit locks (`@Lock`, `@Version`).
2. `JdbcClient`: hand-written SQL, full control.
3. jOOQ: type-safe SQL generated from the schema.

## Decision

Spring Data JPA. Native SQL only where JPA has no equivalent (the advisory lock in ADR 0006).

## Evidence

- In 126 Brazilian Java postings, JPA, Hibernate or Spring Data appear in 35.7%; jOOQ in 1.6%, plain JDBC in 0.8%, MyBatis in 0%.
- No public survey with percentages for Java persistence frameworks was found (JetBrains 2025, JRebel 2025, the 2022 Mihalcea poll only as an image). The Gupy sample is the only number.

## Consequences

- Hibernate behavior to master and explain: flush order, first-level cache, `open-in-view` (disabled), lazy loading.
- `ddl-auto=validate`: Flyway owns the schema, Hibernate fails startup on any mismatch.
- Entities with client-assigned ids implement `Persistable` to avoid a `merge` and its extra `SELECT`.
