package tech.lucassantana.ledger.idempotency;

public class IdempotencyKeyInProgressException extends RuntimeException {

    public IdempotencyKeyInProgressException(String key) {
        super("a request with idempotency key '" + key + "' is still being processed; retry later");
    }
}
