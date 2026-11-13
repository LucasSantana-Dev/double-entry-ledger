# 0011. JMeter, driven from Java with jmeter-java-dsl, generates the load

- Status: accepted
- Date: 2026-11-13

## Context

Phase 2 needs HTTP load against the running service: half of the phase compares virtual and platform threads in Tomcat, which an in-process harness cannot see. The criterion set for this project is what Brazilian Java job postings ask for.

## Options

1. JMeter through jmeter-java-dsl: the test plan is Java code in a JUnit test, run by `./mvnw`, no `.jmx` XML in the repo.
2. Gatling Java DSL: active project, asynchronous load generator, HTML report with percentiles.
3. k6: JavaScript scripts, Go binary outside Maven.
4. Own harness calling `TransferService` directly, latency in HdrHistogram. No HTTP, so no thread mode comparison.

## Evidence

Gupy sample rerun on 2026-11-13 with load testing terms added to `tools/market/gupy_java_sample.py` (see `docs/market/2026-11-13-gupy-java-sample.md`):

| Term | Java postings mentioning it (of 100) |
|---|---|
| JMeter | 11 |
| k6 | 1 |
| Gatling | 0 |
| JMH | 0 |
| "teste de carga / performance" or English equivalents | 4 |

Maven Central on 2026-11-13: Gatling 3.16.0 (September 2026), JMeter 5.6.3 (January 2024), jmeter-java-dsl 1.29.1 (October 2024), JMH 1.37 (August 2023).

## Decision

Option 1. JMeter is the only load tool with a visible signal in the postings, and the Java DSL keeps the plan reviewable as code.

## Consequences

- The load plan lives in a separate Maven profile so `./mvnw verify` stays fast and does not run benchmarks.
- Risk: jmeter-java-dsl has had no release since October 2024 and JMeter 5.6.3 ships Groovy 3, which may not run on Java 25. A spike of at most one hour checks it first. If it fails, this ADR is superseded by one choosing Gatling (option 2), with the failure recorded.
- JMeter uses one thread per virtual user, so at 1,024 users the load generator costs more CPU than Gatling would. It runs in its own JVM, and its CPU use is reported next to the results.

## Spike result (2026-11-13)

The risk above did not materialize. `JmeterSpikeBenchmark` (`./mvnw test -Pbenchmark`) ran on JDK 25.0.5 with jmeter-java-dsl 1.29.1 against this Spring Boot 4.1.2 app over HTTP: 400 transfers from 8 JMeter threads, 0 errors, each request with its own `Idempotency-Key` from the JMeter function `${__UUID()}` and its amount set by a Java lambda in a `jsr223PreProcessor`. The lambda runs in process without Groovy, which is how the real plan will pick the hot or a random account per request. The default `./mvnw verify` does not compile or run anything from the `benchmark` profile.

One finding for the plan: in the spike, JMeter and the application share one JVM. The real benchmark must start the application in its own JVM per cell, because the thread grid reports the application's live threads, heap and CPU, and 1,024 JMeter threads in the same process would pollute them.
