package tech.lucassantana.ledger.outbox;

import java.time.Instant;
import java.util.UUID;
import tools.jackson.databind.JsonNode;

/**
 * The envelope every event travels in (ADR 0018). {@code sequence} is the outbox row id: per
 * account it follows commit order (ADR 0016). The relay passes the stored payload through without
 * knowing its type.
 */
public record LedgerEvent(
        UUID eventId,
        String eventType,
        int schemaVersion,
        Instant occurredAt,
        String aggregateType,
        UUID aggregateId,
        long sequence,
        JsonNode payload) {
}
