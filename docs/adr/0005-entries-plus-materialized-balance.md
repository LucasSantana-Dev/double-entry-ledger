# 0005. Immutable entries plus a materialized balance

- Status: accepted
- Date: 2026-11-13

## Context

Double entry: every transfer writes a debit and a credit that sum to zero, and entries are never edited. Where does the balance live?

## Options

1. Entries only; the balance is `SUM(entries)` at read time.
2. Entries plus a `balance_cents` column, both written in the same transaction.
3. Balance only, no entries.

## Decision

Option 2.

## Reasoning

- Option 1 is always consistent, but reads grow with history, and the funds check still needs to lock something: a `SUM` cannot be locked, so the account row gets locked anyway.
- Option 3 has no audit trail. It is not a ledger.
- Option 2 reads in O(1), the account row is the natural lock target (ADR 0007), and the redundancy is checked: the invariant test asserts `balance = SUM(entries)` for every account, and phase 3 adds a reconciliation job.

## Consequences

- Entries are append-only at three levels: no update path in the code, `@Immutable` in Hibernate, and a trigger that rejects `UPDATE` and `DELETE`.
- A database `CHECK` keeps customer balances non-negative as a backstop for the service rule.
