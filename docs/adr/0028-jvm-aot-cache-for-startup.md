# 0028. Faster startup with the JVM AOT cache

- Status: accepted
- Date: 2026-11-16

## Context

Startup time matters wherever compute is billed per second or scaled to zero, and it is part of the footprint story of ADR 0026. The image is 176 MB of content today (ADR 0024 implementation).

## Options

1. The JVM ahead-of-time cache (JEP 483 in Java 24, made easier to create by JEP 514 in Java 25): a training run during the image build records classes already loaded and linked, and the runtime starts with `-XX:AOTCache`.
2. Option 1 plus Spring AOT processing (bean definitions generated at build time).
3. A GraalVM native image.

## Evidence

- [JEP 483](https://openjdk.org/jeps/483) and [JEP 514](https://openjdk.org/jeps/514) need no code change; the cache is tied to the exact JDK build and class path, which a Docker image fixes.
- Spring AOT evaluates `@Conditional` beans at build time, so the concurrency strategy chosen by `ledger.concurrency.strategy` (ADR 0010) would be fixed in the image.
- A native image needs reflection metadata for Hibernate and Kafka, builds much more slowly, and the JVM benchmark would no longer describe what runs.

## Decision

Option 1, decided by the repo owner. The Dockerfile runs a training start of the application to create the cache and the runtime uses it. Startup time (process start to readiness) is measured with and without the cache. The image size is measured too, and reduced where it costs nothing (for example, files the runtime does not need).

## Consequences

- No change to the code or the tests; the strategy stays selectable at runtime.
- The image build gets slower and the image grows by the cache size; both are measured.
- The training run must not need PostgreSQL or Kafka, or must have them; the implementation settles this.
