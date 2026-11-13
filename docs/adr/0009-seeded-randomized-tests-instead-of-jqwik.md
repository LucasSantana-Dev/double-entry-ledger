# 0009. Seeded randomized tests instead of jqwik

- Status: accepted
- Date: 2026-11-13

## Context

The plan listed jqwik for property-based tests. jqwik 1.10.2 (October 2026) depends on JUnit Platform 1.14.4, while Spring Boot 4.1 manages JUnit 6. Mixing the two platform lines risks a test engine that fails to start or silently runs nothing.

## Decision

Write the properties as plain JUnit tests driven by a seeded `java.util.Random`:

- `MoneyTest`: format then parse returns the same cents for 100,000 random amounts across the whole Pix range.
- `ConcurrentTransfersInvariantTest`: a random concurrent workload, then the seven invariants of the spec checked in the database.

Seed and size come from system properties, so any failure reproduces exactly.

## Consequences

- No shrinking: a failing case is reported at full size rather than minimized. Acceptable while workloads are reproducible by seed.
- Revisit when jqwik ships a JUnit 6 compatible engine.
