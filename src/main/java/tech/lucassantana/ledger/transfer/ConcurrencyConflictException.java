package tech.lucassantana.ledger.transfer;

/**
 * The concurrency strategy aborted on conflict on every attempt (ADR 0010). Nothing was written;
 * the client retries later with the same idempotency key.
 */
public class ConcurrencyConflictException extends RuntimeException {

    public ConcurrencyConflictException(int attempts, Throwable lastConflict) {
        super("the transfer conflicted with concurrent transfers " + attempts + " times; retry later", lastConflict);
    }
}
