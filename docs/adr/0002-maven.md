# 0002. Maven as the build tool

- Status: accepted
- Date: 2026-11-13

## Context

The build tool is the first file a reviewer sees (`pom.xml` or `build.gradle.kts`). It rarely decides an interview, so recognition by the target reader weighs most.

## Options

1. Maven
2. Gradle (Kotlin DSL)

## Decision

Maven, through the wrapper (`./mvnw`), so nothing needs to be installed besides a JDK.

## Evidence

- 67% of Java developers use Maven ([JetBrains State of Java 2025](https://lp.jetbrains.com/the-state-of-java-2025)). No 2025 Gradle share was found.
- In 126 Brazilian Java postings on Gupy, Maven is named in 22 and Gradle in 6: 79% of the postings that name a tool ask for Maven.

## Consequences

Slower repeated builds than Gradle's build cache. Irrelevant at this size.
