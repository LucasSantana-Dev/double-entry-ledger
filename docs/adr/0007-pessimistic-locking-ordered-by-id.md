# 0007. Pessimistic row locks, acquired in id order

- Status: accepted for phase 1; phase 2 decides the final strategy by measurement
- Date: 2026-11-13

## Context

Two transfers of 80.00 leave an account holding 100.00 at the same time. Both read 100.00, both debit, and the balance ends at -60.00 (lost update).

## Options

1. Pessimistic: `SELECT ... FOR UPDATE` on both accounts.
2. Optimistic: a `version` column, retry on conflict.
3. Conditional update: `UPDATE ... SET balance = balance - x WHERE id = ? AND balance >= x`, check the row count.
4. `SERIALIZABLE` isolation, retry on serialization failure.

## Decision

Option 1 for phase 1, because it is the easiest to prove correct. Both rows are locked in a global order (smaller UUID first). Without the order, A to B and B to A running together each lock their source first and deadlock.

## Evidence plan

Phase 2 runs the four options under two loads (uniform accounts, one hot account) and picks by p99 latency, throughput and abort rate. Until then the evidence is the phase 1 invariant test: thousands of concurrent transfers in both directions with zero deadlocks and zero invariant violations.

## Consequences

Transfers touching the same account run one at a time. That is the cost phase 2 will put a number on.
