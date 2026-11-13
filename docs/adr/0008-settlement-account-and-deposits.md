# 0008. Money enters through a settlement account

- Status: accepted
- Date: 2026-11-13

## Context

In double entry every cent leaves some account. Money arriving from outside the ledger (a bank transfer in, a card top-up) needs a source.

## Options

1. A settlement account, internal to the system, that may go negative. A deposit is a transfer from it to a customer.
2. An initial balance set when the account is opened.

## Decision

Option 1. `POST /deposits` moves money from the settlement account through the same code path as a transfer: same locks, same idempotency, same entries.

## Reasoning

Option 2 makes money appear without an entry. That breaks the strongest invariant the project tests (the sum of all balances is zero) and leaves money without an audit trail. With option 1, the settlement balance is exactly minus the money that entered, so `SUM(all balances) = 0` holds after any workload.

## Consequences

- The settlement account is created by the migration with a fixed id and is internal: `GET /accounts/{id}` answers 404 for it, and `/transfers` rejects it as source or destination with 422.
- Withdrawals, the reverse direction, are out of scope for phase 1.
