# 0004. Money as `long` cents, Pix decimal string at the API

- Status: accepted
- Date: 2026-11-13

## Context

`double` cannot hold money: `0.1 + 0.2` is `0.30000000000000004`. The choice is between integer minor units and `BigDecimal`.

## Options

1. `long` cents in the domain and `BIGINT` in the database.
2. `BigDecimal` end to end and `NUMERIC(19,2)` in the database.

## Decision

Option 1. The API accepts and returns the API Pix decimal string (`"500.00"`), and the conversion happens at the boundary in `Money.parse`.

## Evidence

- Stripe: "enter 1099 to charge 10.99 USD", integer minor units ([docs](https://docs.stripe.com/currencies)).
- Adyen: amounts in minor units, GBP 10 = 1000 ([docs](https://docs.adyen.com/development-resources/currency-codes/)).
- TigerBeetle: amounts are unsigned 128-bit integers, mapping "the smallest useful unit" to 1 ([docs](https://docs.tigerbeetle.com/coding/data-modeling/)).
- Banco Central's API Pix: `valor.original` is a string with pattern `\d{1,10}\.\d{2}`, example `"500.00"` ([bacen/pix-api](https://github.com/bacen/pix-api)). Its maximum, 9,999,999,999.99, fits a `long` with room to spare (about 92 quadrillion reais).

## Consequences

- Exact arithmetic with `Math.addExact` and `Math.subtractExact`, which throw instead of overflowing silently.
- No `BigDecimal` traps (`equals` depends on scale, mandatory `RoundingMode`).
- BRL only. Another currency needs its scale stored next to the amount, as TigerBeetle does per ledger.
