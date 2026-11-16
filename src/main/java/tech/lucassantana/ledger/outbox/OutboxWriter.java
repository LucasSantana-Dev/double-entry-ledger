package tech.lucassantana.ledger.outbox;

import io.micrometer.tracing.Span;
import io.micrometer.tracing.Tracer;
import io.micrometer.tracing.propagation.Propagator;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import tools.jackson.databind.json.JsonMapper;

/**
 * Writes the outbox row of a transfer inside the caller's transaction (ADR 0016), so the event
 * commits or rolls back with the transfer. Refuses to run outside a transaction: an event
 * committed on its own would be the dual write the outbox exists to avoid.
 *
 * <p>The row also keeps the W3C {@code traceparent} of the current span (ADR 0021), so the relay
 * can send the event inside the same trace long after the request ended.
 */
@Component
public class OutboxWriter {

    private static final Logger log = LoggerFactory.getLogger(OutboxWriter.class);
    static final String TRACEPARENT = "traceparent";

    private final JdbcClient jdbc;
    private final JsonMapper json;
    private final Tracer tracer;
    private final Propagator propagator;

    OutboxWriter(JdbcClient jdbc, JsonMapper json, Tracer tracer, Propagator propagator) {
        this.jdbc = jdbc;
        this.json = json;
        this.tracer = tracer;
        this.propagator = propagator;
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
                            (event_id, aggregate_type, aggregate_id, event_type, schema_version, payload, occurred_at,
                             traceparent)
                        values (:eventId, :aggregateType, :aggregateId, :eventType, :schemaVersion,
                                cast(:payload as jsonb), :occurredAt, :traceparent)
                        """)
                .param("eventId", UUID.randomUUID())
                .param("aggregateType", TransferCommitted.AGGREGATE_TYPE)
                .param("aggregateId", transferId)
                .param("eventType", TransferCommitted.EVENT_TYPE)
                .param("schemaVersion", TransferCommitted.SCHEMA_VERSION)
                .param("payload", json.writeValueAsString(payload))
                .param("occurredAt", occurredAt.atOffset(ZoneOffset.UTC))
                .param("traceparent", traceparent())
                .update();
    }

    /**
     * The current span as a W3C {@code traceparent}, sampled flag included, or null when there is
     * no span. Tracing must never fail a transfer, so any error here only loses the context.
     */
    private String traceparent() {
        try {
            Span span = tracer.currentSpan();
            if (span == null) {
                return null;
            }
            Map<String, String> carrier = new HashMap<>();
            propagator.inject(span.context(), carrier, Map::put);
            return carrier.get(TRACEPARENT);
        } catch (RuntimeException e) {
            log.debug("outbox row written without trace context: {}", e.toString());
            return null;
        }
    }
}
