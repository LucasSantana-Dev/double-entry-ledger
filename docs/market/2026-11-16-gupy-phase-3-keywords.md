# Brazilian Java job postings, phase 3 keywords (2026-11-16)

Evidence for ADRs 0016 to 0019. Reproduce with `python tools/market/gupy_phase3_keywords.py`.

## Method

- Source: the public search endpoint of portal.gupy.io, `api/job-search/jobs?jobName=java`, fetched on 2026-11-16.
- Kept every posting whose title contains the word "Java" (case-insensitive): 100 postings from 47 companies.
- Counted case-insensitive regular expression matches in title plus description (HTML stripped). The patterns are in the script, including the Portuguese forms (for example `orientado a eventos`, `resiliência`).

## Results

| Group | Keyword | Share of the 100 postings |
|---|---|---|
| Messaging | Kafka | 36% |
| | RabbitMQ | 20% |
| | SQS or SNS | 12% |
| Architecture | event-driven | 17% |
| | resilience (any sense) | 21% |
| | idempotency | 7% |
| | CDC | 2% |
| | outbox | 2% |
| | saga | 2% |
| Serialization | Protobuf | 1% |
| | Avro | 0% |
| | Schema Registry | 0% |
| Tools | Testcontainers | 1% |
| | Debezium | 0% |
| | Toxiproxy | 0% |
| | chaos | 0% |
| | Spring Modulith | 0% |
| | ShedLock | 0% |
| Observability | Grafana | 10% |
| | Prometheus | 5% |
| | OpenTelemetry | 4% |

## Caveats

- n = 100 from one portal on one day. Counts of 0 to 2 are noise: they say a term is rare, not that it is absent from the market.
- A keyword match does not separate required skills from nice-to-haves, nor a technology the team uses from one the posting merely mentions.
- "resilience" matches the soft-skill sense too ("resiliência" as a personal trait), hence "any sense".
- Postings change daily; a rerun will give different numbers.

## How the numbers were used

- Kafka at 36% is the clearest signal: phase 3 publishes to Kafka (ADR 0016).
- Outbox, CDC and Debezium are rare as words, so the outbox mechanism is chosen for what it shows and costs, not for demand (ADR 0016).
- Idempotency (7%) and event-driven (17%) back the idempotent consumer (ADR 0017).
- Avro, Schema Registry and Protobuf near zero back JSON events (ADR 0018).
- No posting names a fault-injection tool; resilience as a requirement backs testing failures with the cheapest tools that produce real ones (ADR 0019).
