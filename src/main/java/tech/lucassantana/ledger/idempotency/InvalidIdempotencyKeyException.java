package tech.lucassantana.ledger.idempotency;

public class InvalidIdempotencyKeyException extends RuntimeException {

    public InvalidIdempotencyKeyException() {
        super("Idempotency-Key must be between 1 and 255 non-blank characters");
    }
}
