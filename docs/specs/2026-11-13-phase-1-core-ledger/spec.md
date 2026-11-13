---
status: done
date: 2026-11-13
phase: 1
tags: [ledger, idempotency, concurrency, postgres]
---

# Phase 1: core ledger

## Goal

A transfer service whose central guarantee is that money is never created, lost or duplicated, even under concurrent requests and client retries. The guarantee is proven by tests against a real PostgreSQL, not asserted.

## Scope

- Open a customer account. Read its balance.
- Deposit into a customer account (money entering from outside, see ADR 0008).
- Transfer between two customer accounts.
- Every write requires an `Idempotency-Key` header.

Out of scope for this phase: messaging and outbox (phase 3), load benchmarks and lock strategy comparison (phase 2), observability and deployment (phase 4), authentication, multiple currencies (BRL only), withdrawals, statements.

## API

| Method | Path | Success | Errors |
|---|---|---|---|
| POST | `/accounts` | 201 + account | |
| GET | `/accounts/{id}` | 200 + account | 404 |
| POST | `/deposits` | 201 + deposit | 400, 404, 409, 422 |
| POST | `/transfers` | 201 + transfer | 400, 404, 409, 422 |

Amounts travel as the API Pix decimal string, pattern `\d{1,10}\.\d{2}` (for example `"500.00"`). Errors are RFC 9457 problem details with a `type` per failure:

| Status | Type | When |
|---|---|---|
| 400 | `invalid-amount` | amount does not match the Pix pattern |
| 400 | `invalid-idempotency-key` | key blank or longer than 255 characters |
| 400 | (framework) | missing header, malformed body |
| 404 | `account-not-found` | unknown account, or a non-customer account |
| 409 | `idempotency-key-in-progress` | same key while the first request is still running |
| 422 | `idempotency-key-reused` | same key with a different payload |
| 422 | `insufficient-funds` | source balance lower than the amount |
| 422 | `invalid-transfer` | zero amount, same source and destination, settlement account used directly |

A retry with a key whose transfer already committed returns the original response with `201` and the header `Idempotent-Replayed: true`.

## Invariants (the acceptance criteria)

After any workload, including concurrent transfers in both directions, a hot account and duplicated submissions:

1. The sum of all balances, settlement account included, is zero.
2. No customer balance is negative.
3. Every account balance equals the sum of its entries.
4. Every transfer has exactly two entries, and they sum to zero.
5. No idempotency key produced more than one transfer, and every replay returned the original transfer. A key whose attempt failed (for example, insufficient funds) holds nothing, so a later retry with it may legitimately succeed (ADR 0006).
6. The number of transfers equals the number of stored idempotency keys.
7. Entries cannot be updated or deleted (rejected by the database).

## Decisions

Recorded in `docs/adr/0001` to `0009`. Each one states the option chosen, the alternatives and the evidence.

## Done when

- `./mvnw verify` passes, with the invariant test running at least 5,000 concurrent operations on virtual threads.
- README states what the invariant test proves and how to run it.
