package tech.lucassantana.ledger.statement;

import java.time.Instant;
import java.util.UUID;

/**
 * The consumer's own view of the event envelope (ADR 0018). It shares the JSON contract with the
 * producer, not a class, so the statement could move to another service unchanged.
 */
public record TransferEvent(
        UUID eventId,
        String eventType,
        int schemaVersion,
        Instant occurredAt,
        UUID aggregateId,
        long sequence,
        Payload payload) {

    public record Payload(UUID transferId, UUID fromAccountId, UUID toAccountId, long amountCents) {
    }
}
