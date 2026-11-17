# 0027. Memory target: the whole stack in 2 GB

- Status: accepted
- Date: 2026-11-16

## Context

Memory decides the machine size, and the machine size decides the cost (ADR 0026). Today the Kubernetes limits add up to about 2.5 GB: the application 1 GiB with `-XX:MaxRAMPercentage=75`, Kafka 1 GiB with a 512 MB heap, PostgreSQL 512 MiB. Nobody has measured what the stack actually uses.

## Options

1. Fit the application, PostgreSQL and Kafka in 2 GB in total.
2. Fit them in 1 GB.
3. Measure and remove only obvious waste, with no target.

## Evidence

- A 2 GB machine costs about half of a 4 GB one: t4g.small $12.26 against t4g.medium $24.53 a month in us-east-1; Lightsail $12 against $24 (sources in ADR 0026).
- 1 GB would leave a few hundred MB per process for a JVM application, a JVM broker and PostgreSQL; it likely needs a native image (rejected in ADR 0028) or a broker other than Kafka.

## Decision

Option 1, decided by the repo owner.

- Measure first: peak resident memory per container and startup time, under a fixed load, before and after each change.
- Then cut, for example: JVM heap and metaspace sizing for the application, the garbage collector choice for a small heap, the Hikari pool size, the Kafka broker heap, PostgreSQL `shared_buffers` and connection limits.
- Prove it: with a 2 GB total limit (per-container limits that add up to 2 GB), the smoke test and a short load run pass with no out-of-memory kill. If the stack does not fit, the report says by how much and why.
- The phase 1 to 4 tests stay green; they do not run under these limits.

## Consequences

- The cost map can point at the 2 GB machines.
- Smaller heaps can cost throughput; ADR 0029 measures it.
