# 0018. JSON event envelope with Jackson 3

- Status: accepted
- Date: 2026-11-16

## Context

The relay (ADR 0016) and the statement consumer (ADR 0017) need a wire format for the transfer event, with enough metadata to deduplicate, order and evolve it.

## Options

1. JSON through Spring for Apache Kafka's Jackson 3 serializers (`JacksonJsonSerializer`, `JacksonJsonDeserializer`), with a fixed envelope.
2. Avro with a Schema Registry.
3. Protobuf.

## Evidence

- Market (Gupy sample of 100 Java postings, 2026-11-16, [docs/market/2026-11-16-gupy-phase-3-keywords.md](../market/2026-11-16-gupy-phase-3-keywords.md)): Avro 0%, Schema Registry 0%, Protobuf 1%, Kafka 36%.
- Spring for Apache Kafka 4 (managed by Spring Boot 4.1.2 at 4.1.2) prefers Jackson 3 and ships `JacksonJsonSerializer` and `JacksonJsonDeserializer` ([change history](https://docs.spring.io/spring-kafka/reference/appendix/change-history.html)); Boot already configures Jackson 3 for the web layer.
- Avro with Confluent's serializers needs Confluent's Maven repository (they are not on Maven Central) and a registry container; Testcontainers has no official Schema Registry module ([modules](https://java.testcontainers.org/modules/kafka/)), so CI would carry a hand-written generic container.

## Decision

Option 1. Topic `ledger.transfers`, record key the transfer id (ADR 0016), value:

```json
{
  "eventId": "5d1c...",
  "eventType": "TransferCommitted",
  "schemaVersion": 1,
  "occurredAt": "2026-11-16T12:00:00.123456Z",
  "aggregateType": "transfer",
  "aggregateId": "<transfer id>",
  "sequence": 1234,
  "payload": {
    "transferId": "<transfer id>",
    "fromAccountId": "<account id>",
    "toAccountId": "<account id>",
    "amountCents": 50000
  }
}
```

- `sequence` is the outbox row id: per account, it follows commit order (ADR 0016).
- Amounts are `long` cents, the internal representation of ADR 0004. The Pix decimal string belongs to the public HTTP API; events are internal and are consumed by code that does arithmetic.
- The producer sends no Java type headers and the consumer deserializes into its own envelope type, so the two share a JSON contract, not a class. `schemaVersion` changes when a field changes meaning; adding an optional field keeps version 1.

## Consequences

- Human-readable events in `kafka-console-consumer` and in test failures.
- No schema enforcement at the broker: compatibility is a code review rule plus `schemaVersion`. A registry becomes worth its cost when other teams consume the topic.
- Larger messages than Avro or Protobuf; irrelevant at this volume.
