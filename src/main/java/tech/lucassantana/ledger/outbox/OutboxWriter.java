package tech.lucassantana.ledger.outbox;

import java.time.Instant;
import java.time.ZoneOffset;
import java.util.UUID;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import tools.jackson.databind.json.JsonMapper;

/**
 * Writes the outbox row of a transfer inside the caller's transaction (ADR 0016), so the event
 * commits or rolls back with the transfer. Refuses to run outside a transaction: an event
 * committed on its own would be the dual write the outbox exists to avoid.
 */
@Component
public class OutboxWriter {

    private final JdbcClient jdbc;
    private final JsonMapper json;

    OutboxWriter(JdbcClient jdbc, JsonMapper json) {
        this.jdbc = jdbc;
        this.json = json;
    }

    /**
     * Call it after the transfer has locked or read both accounts: the row's identity, the ledger
     * sequence, must be drawn after every earlier transfer on those accounts committed.
     */
    @Transactional(propagation = Propagation.MANDATORY)
    public void transferCommitted(UUID transferId, UUID fromAccountId, UUID toAccountId, long amountCents,
            Instant occurredAt) {
        var payload = new TransferCommitted(transferId, fromAccountId, toAccountId, amountCents);
        jdbc.sql("""
                        insert into outbox_events
                            (event_id, aggregate_type, aggregate_id, event_type, schema_version, payload, occurred_at)
                        values (:eventId, :aggregateType, :aggregateId, :eventType, :schemaVersion,
                                cast(:payload as jsonb), :occurredAt)
                        """)
                .param("eventId", UUID.randomUUID())
                .param("aggregateType", TransferCommitted.AGGREGATE_TYPE)
                .param("aggregateId", transferId)
                .param("eventType", TransferCommitted.EVENT_TYPE)
                .param("schemaVersion", TransferCommitted.SCHEMA_VERSION)
                .param("payload", json.writeValueAsString(payload))
                .param("occurredAt", occurredAt.atOffset(ZoneOffset.UTC))
                .update();
    }
}
