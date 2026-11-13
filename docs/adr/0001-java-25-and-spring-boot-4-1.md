# 0001. Java 25 and Spring Boot 4.1

- Status: accepted
- Date: 2026-11-13

## Context

A new project picks a Java LTS and a Spring Boot line it will live on. Phase 2 benchmarks virtual threads against platform threads on a JDBC workload.

## Options

1. Java 25 + Spring Boot 4.1
2. Java 21 + Spring Boot 4.1
3. Java 21 + Spring Boot 3.5

## Decision

Option 1.

## Evidence

- Spring Boot 3.5 open source support ended on 2026-06-30; 3.5.16 was its last OSS release ([spring.io](https://spring.io/blog/2026/06/25/spring-boot-3-5-16-available-now/)). Boot 4.0 OSS ends 2026-12-31 and 4.1 ends 2027-07-31 ([endoflife.date](https://endoflife.date/spring-boot)). A project started today on 3.5 gets no free security patches.
- Developers use Java 21 (40%) and 17 (39%) the most ([JetBrains State of Java 2025](https://lp.jetbrains.com/the-state-of-java-2025), more than 5,000 respondents). Job postings barely name a version: in 126 Brazilian Java postings, Java 21 appears in 0.8% and Java 25 in 0% (see `docs/market/2026-11-13-gupy-java-sample.md`). The version is not a recruiter keyword, so the technical argument decides.
- JEP 491 (delivered in JDK 24) stopped virtual threads from pinning their carrier thread inside `synchronized`. Java 25 is the first LTS where virtual threads do not depend on every library having replaced `synchronized`. That is what phase 2 measures.

## Consequences

- `spring.threads.virtual.enabled=true` from day one.
- Boot 4 brings JUnit 6 and Jackson 3, which drove ADR 0009.
- Upgrade to the next Boot minor before 2027-07-31.
