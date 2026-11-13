# 0006. Idempotency key stored in PostgreSQL, in the transfer's transaction

- Status: accepted
- Date: 2026-11-13

## Context

A client sends a transfer, the connection drops before the response, and the client retries. Without protection the money moves twice. Clients send an `Idempotency-Key` header, the convention from Stripe that the IETF draft `httpapi-idempotency-key-header` (version 08) standardizes.

## Decisions

### Where the key lives

Options: (a) a PostgreSQL table written in the transfer's transaction; (b) Redis with a TTL; (c) a business key such as Pix's `endToEndId`.

Chosen: (a). The key and the transfer commit together or not at all. Redis sits outside the transaction, so there is a window where the transfer committed and the key did not, which is exactly the hole this project claims not to have.

### A duplicate arrives while the first request is still running

Options: answer 409 immediately, or wait for the first request and replay its result.

Chosen: 409. The draft says the server "SHOULD reply with an HTTP 409 status code" when "the request is retried, while the original request is still being processed", and Stripe answers 409 too. Implementation: `pg_try_advisory_xact_lock(hashtextextended(key, 0))`, which never blocks and is released on commit or rollback, so a crash cannot leave a key stuck. A 64-bit hash collision between two different keys can only cause a spurious 409, which the client retries.

### The same key arrives with a different payload

422 `idempotency-key-reused`, as the draft recommends. The request fingerprint is `operation:from:to:cents`.

### Failures are not memoized

Options: memoize every response, errors included (Stripe API v1), or bind keys only to committed transfers.

Chosen: only committed transfers. A failure such as insufficient funds rolls back the key with everything else, and a retry with the same key is evaluated again (for example after a deposit). This matches Stripe API v2, where retries complete failed work when possible, and it avoids storing response bodies.

## Consequences

- A replay returns 201 with the original body and `Idempotent-Replayed: true`.
- Keys are kept forever in phase 1. Stripe prunes keys after at least 24 hours; a retention job is future work.
