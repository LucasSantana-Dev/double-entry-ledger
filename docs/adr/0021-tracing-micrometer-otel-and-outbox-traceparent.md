# 0021. Tracing with Boot's OpenTelemetry starter and a traceparent in the outbox row

- Status: accepted
- Date: 2026-11-16

## Context

A transfer crosses an HTTP request, a database commit, the outbox relay (ADR 0016, a `@Scheduled` batch every 100 ms on another thread) and the statement consumer (ADR 0017). Phase 4 needs one trace per transfer, from the request to the statement line. Traces usually break at the relay, because by the time it sends, the request that wrote the row has long finished.

## Options

1. `spring-boot-starter-opentelemetry` (Micrometer Tracing with the OpenTelemetry bridge, OTLP export), Kafka observation on the template and the listener, `datasource-micrometer` for JDBC spans, and a `traceparent` column on `outbox_events` that the relay restores as the parent of each send span.
2. The OpenTelemetry Java agent: automatic spans for HTTP, JDBC, Kafka and `@Scheduled`, no code.
3. Option 1 without the outbox column: the request trace ends at the commit, the relay and the consumer start their own traces.

## Evidence

- Spring Boot 4 ships `spring-boot-starter-opentelemetry` with the OTel API, `micrometer-tracing-bridge-otel` and `micrometer-registry-otlp`; every Observation becomes a span, so Spring MVC requests are covered ([Spring blog, 2025-11-18](https://spring.io/blog/2025/11/18/opentelemetry-with-spring-boot/), [Boot tracing reference](https://docs.spring.io/spring-boot/reference/actuator/tracing.html)).
- Kafka propagation needs `spring.kafka.template.observation-enabled` and `spring.kafka.listener.observation-enabled`, both `false` by default in Boot 4.1.2 ([application properties](https://docs.spring.io/spring-boot/appendix/application-properties/index.html), [Spring Kafka observation](https://docs.spring.io/spring-kafka/reference/kafka/micrometer.html)).
- JDBC spans are not in Boot. Boot's observability page points to Datasource Micrometer, whose `datasource-micrometer-spring-boot` 2.0.1 is built against Boot 4.0 ([Boot observability](https://docs.spring.io/spring-boot/reference/actuator/observability.html), [Maven Central](https://central.sonatype.com/artifact/net.ttddyy.observation/datasource-micrometer-spring-boot)).
- The OpenTelemetry messaging conventions let a producer attach a creation context to each message; a parent relationship is allowed for single-message processing, links are required for batches ([messaging spans](https://opentelemetry.io/docs/specs/semconv/messaging/messaging-spans/)). Debezium's outbox router does the same with a `tracingspancontext` column restored as the parent ([Debezium tracing](https://debezium.io/documentation/reference/stable/integrations/tracing.html)).
- The Java agent (2.33.1) instruments Kafka, Spring Kafka, JDBC and Spring Scheduling, and is tested on Java 25 ([supported libraries](https://github.com/open-telemetry/opentelemetry-java-instrumentation/blob/main/docs/supported-libraries.md)). It would still need the same column to join the relay to the request. It also has to be attached to the Surefire JVM, and it risks duplicate spans next to Boot's Micrometer instrumentation.
- Default sampling is 10%, parent based (`management.tracing.sampling.probability`); the restored context must carry the sampled flag.
- Market (Gupy, 100 Java postings, 2026-11-16, [docs/market/2026-11-16-gupy-phase-4-keywords.md](../market/2026-11-16-gupy-phase-4-keywords.md)): tracing 10%, OpenTelemetry 4%, Micrometer, Jaeger and Zipkin 0%.

## Decision

Option 1, decided by the repo owner.

- Flyway V5 adds a nullable `traceparent` column to `outbox_events`, filled from the current span when the transfer transaction writes the row.
- The relay sends each row in its own span whose parent is the stored context (one message per span, which the conventions allow); a row without a context gets a new root span.
- Kafka observation on: the W3C `traceparent` header carries the context to the consumer, whose span becomes a child.
- `datasource-micrometer` adds JDBC spans.
- Sampling 1.0 in tests and in the local stack; the deployed value stays at the default.

## Consequences

- One trace shows the request, SQL, commit, the wait in the outbox, the send and the statement write, so the outbox lag of each transfer is visible.
- The outbox row grows by about 55 bytes. Rows written before V5 have no context.
- The relay owns a few lines of tracing code (extract the stored context, open the span). The Micrometer propagation API for this has to be checked against the bridge version during implementation.
- No Java agent in the image or in the tests.
