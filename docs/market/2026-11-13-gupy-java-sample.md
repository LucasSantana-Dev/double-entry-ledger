# Brazilian Java job postings, Gupy sample (2026-11-13)

Evidence for ADRs 0001 to 0004. Reproduce with `python tools/market/gupy_java_sample.py`.

## Method

- Source: the public search endpoint of portal.gupy.io, term `java`.
- Kept every posting whose title contains the word "Java" (not "JavaScript"): 126 postings from 53 companies, 100 of them published from May 2026 on.
- Counted case-insensitive keyword mentions in title, company and description.

## Results

| Group | Item | Share of the 126 postings |
|---|---|---|
| Persistence | JPA, Hibernate or Spring Data | 35.7% |
| | jOOQ | 1.6% |
| | JDBC | 0.8% |
| | MyBatis | 0% |
| Build | Maven | 17.5% |
| | Gradle | 4.8% |
| Java version | 8 | 20.6% |
| | 17 | 5.6% |
| | 21 | 0.8% |
| | 25 | 0% |
| Framework | Spring | 66.7% |
| | Spring Boot | 53.2% |
| | Quarkus | 13.5% |
| Messaging | Kafka | 32.5% |
| | RabbitMQ | 19.0% |
| | SQS | 13.5% |
| Infrastructure | Docker | 42.9% |
| | AWS | 34.9% |
| | Kubernetes | 33.3% |
| | Observability tools | 33.3% |
| Testing | JUnit | 17.5% |
| | TDD | 15.1% |
| | Testcontainers | 0% |
| Finance domain | in title or company name | 9.5% |
| | any mention, benefit phrases removed | 59.5% |

Titles per language on the same portal: java 126, python 19, node and nodejs 12, kotlin 7, golang 4. Java postings tend to put the language in the title while Node postings often say "Backend", so this count overstates the gap.

## Limits

- One portal, skewed toward mid and large companies and consultancies. The largest company holds 11 of the 126 postings.
- Keyword counts do not separate required from nice to have, and a posting that does not name a tool may still use it.
- "banco de dados", "banco de horas" and "seguro de vida" inflated the first finance count to 88%; they are removed before counting.

## Rerun for phase 2: load testing tools (2026-11-13, later the same day)

Evidence for ADR 0011. Same script, with a `load testing` group added. The portal returned 100 postings from 46 companies on this run, fewer than in the morning run above, because postings closed in between.

| Item | Postings mentioning it (of 100) |
|---|---|
| JMeter | 11 |
| k6 | 1 |
| Gatling | 0 |
| JMH | 0 |
| A load or performance testing phrase (Portuguese or English) | 4 |

The sample is small: 11 against 0 shows a direction, not a precise share.
