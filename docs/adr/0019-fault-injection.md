# 0019. Fault injection with Toxiproxy, Docker pause and test hooks

- Status: accepted
- Date: 2026-11-16

## Context

The phase 3 guarantees (ADR 0016, ADR 0017) are about failures: the broker is down, the database connection drops between the Kafka send and the mark-published update, the consumer dies between its database commit and its offset commit, a message arrives twice. A test that only runs the happy path proves none of them.

## Options

1. Per fault, the cheapest injection that produces the real failure:
   - database connection cut: Toxiproxy between the application and PostgreSQL, disabled at the exact moment through a hook;
   - broker down: pause the Kafka container through the Docker API;
   - consumer crash after the side effect and before the offset commit: a test-only hook that throws right after the database commit;
   - duplicate delivery: force the relay to send already published events again.
2. Chaos Monkey for Spring Boot 4.0.0 for all of them.
3. Toxiproxy in front of Kafka as well as PostgreSQL.

## Evidence

- Chaos Monkey for Spring Boot 4.0.0 ([releases](https://github.com/codecentric/chaos-monkey-spring-boot/releases)) is built against Spring Boot 4.0.2 and injects latency, exceptions or kills at the bean level. It cannot cut a network link, so it cannot produce a broken JDBC connection in the middle of a transaction, and it cannot pick the instant between two statements.
- Testcontainers Toxiproxy module ([docs](https://java.testcontainers.org/modules/toxiproxy/)): a TCP proxy container controlled from the test; disabling a proxy closes its open connections, which the JDBC driver sees as a real I/O failure. Image pinned at `ghcr.io/shopify/toxiproxy:2.12.0` (the docs example uses 2.5.0; 2.12.0 is the version tested here).
- Kafka advertises its listener addresses to clients. Proxying it needs an extra listener advertising the proxy address, which the Testcontainers `KafkaContainer` does not set up by default; pausing the container gives an unreachable broker without that.
- Market (Gupy sample, 2026-11-16, [docs/market/2026-11-16-gupy-phase-3-keywords.md](../market/2026-11-16-gupy-phase-3-keywords.md)): resilience (any sense) 21%, Testcontainers 1%, Toxiproxy 0%, chaos 0%. Postings ask for resilient systems, not for a chaos tool by name, so the choice is about what each tool can actually break.

## Decision

Option 1. `FaultInjectionTest` runs on its own containers: PostgreSQL reached only through Toxiproxy, and Kafka. Each scenario writes transfers, injects its fault, lets the system recover and asserts the phase 3 invariants: every event published, the statement applied each event once, reconciliation clean, statement balances equal ledger balances.

Hooks are setters on the relay and the listener (`afterSendHook`, `afterApplyHook`), documented as test seams and no-ops unless a test sets them.

## Consequences

- The database fault is real (a dropped TCP connection mid-transaction), not a mocked exception.
- The hooks are production code paths that do nothing in production; they are the price of choosing the exact failure instant.
- Rejected Toxiproxy for Kafka: latency or bandwidth faults on the broker link are not covered; pausing covers "unreachable".
- CI cost: estimated at 2 to 4 minutes before implementation; the measured number is recorded below.

## Measured cost

Local run, 2026-11-16, Windows 11 laptop, JDK 25, Docker Desktop, images already pulled:

| Build | Tests | `./mvnw verify` |
|---|---|---|
| Before phase 3 (`main` at 6a59f6d) | 55 | 1 min 40 s |
| After phase 3 | 69 | 3 min 27 s |
| of which `FaultInjectionTest` alone | 4 | 50 s |

Phase 3 adds about 1 min 47 s: 50 s for the fault scenarios (most of it the broker pause, which waits for the producer's 8 s delivery timeout), the rest is a Kafka container per Spring test context and the invariant test draining 4,000 events per strategy into the statement. Inside the 2 to 4 minute estimate and below the 6 minute budget, so the fault tests stay in the default `verify`; no separate profile.
