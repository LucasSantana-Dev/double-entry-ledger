# 0020. Conditional update as the default concurrency strategy

- Status: accepted; supersedes the default of ADR 0007 (pessimistic stays selectable)
- Date: 2026-11-16

## Context

Phase 2 (ADR 0010) made the concurrency strategy selectable with `ledger.concurrency.strategy` and measured the four options. The default, used when the property is not set, was still the phase 1 choice: `SELECT ... FOR UPDATE` in id order (ADR 0007). ADR 0007 chose it because it was the easiest to prove correct and left the final pick to the phase 2 numbers. All four strategies pass the invariant test, so correctness no longer separates them.

## Options

1. Conditional update: one `UPDATE ... SET balance_cents = balance_cents - x WHERE id = ? AND balance_cents >= x` per account, in id order, checking the row count (`AccountRepository.debitIfCovered`).
2. Keep pessimistic locking as the default.
3. Optimistic or `SERIALIZABLE` as the default.

## Evidence

Phase 2 benchmark, one GitHub Actions run, 256 users on virtual threads, medians of 5 repetitions, 60 s measured per cell (README "Benchmark results", `docs/benchmarks/2026-11-13/results.md`). "Hot" is the share of transfers touching one shared account.

| strategy | created/s 0% / 30% / 90% hot | p99 ms (201) at 90% hot | 503 per minute at 90% hot |
|---|---|---|---|
| conditional-update | 2,042 / 1,602 / 983 | 337 | 0 |
| pessimistic | 1,878 / 1,458 / 768 | 416 | 0 |
| optimistic | 1,813 / 1,043 / 292 | 745 | 16,158 |
| serializable | 1,557 / 808 / 228 | 917 | 13,186 |

- Conditional update leads in every cell: 9%, 10% and 28% more transfers per second than pessimistic at 0%, 30% and 90% hot, and a 19% lower p99 at 90% hot. It saves a round trip per account (no `SELECT ... FOR UPDATE` before the `UPDATE`).
- Like pessimistic, it waits on the row lock instead of aborting, so it never returned 503.
- Optimistic and `SERIALIZABLE` abort on conflict and collapse under a hot account: 3 to 4 times fewer transfers than conditional update at 90% hot and thousands of 503 per minute. They are out.

## Decision

Option 1. `ConditionalUpdateStrategy` carries `matchIfMissing = true`; `PessimisticStrategy` now needs `ledger.concurrency.strategy=pessimistic`. Decided by the repo owner from the numbers above.

## Consequences

- The business rule "the balance covers the debit" lives in the SQL predicate, not in the `Account` entity. The settlement account rule (ADR 0008) is a predicate on the same statement too.
- When the `UPDATE` matches no row, the strategy reads the account to tell the client why (not found, insufficient funds, settlement not allowed). That read runs only on the failure path.
- Pessimistic remains available and tested; the invariant test and the error path test still run every strategy.
- `TransferApiTest` checks that a context without the property gets `ConditionalUpdateStrategy`, so the default cannot change silently. Every test that runs without the property (API, phase 3 outbox, statement and fault injection tests) now exercises conditional update.
