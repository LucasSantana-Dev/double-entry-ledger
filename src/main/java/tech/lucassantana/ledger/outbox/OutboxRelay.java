package tech.lucassantana.ledger.outbox;

import io.micrometer.tracing.Span;
import io.micrometer.tracing.Tracer;
import io.micrometer.tracing.propagation.Propagator;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.errors.RecordBatchTooLargeException;
import org.apache.kafka.common.errors.RecordTooLargeException;
import org.apache.kafka.common.errors.SerializationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.json.JsonMapper;

/**
 * Moves outbox rows to Kafka (ADR 0016). Each batch is one database transaction: take the relay
 * advisory lock, claim the oldest pending rows, send them, wait for every acknowledgement, mark
 * the acknowledged rows published. Delivery is at least once: if the transaction fails after a
 * send (crash, lost connection), the rows stay pending and are sent again.
 *
 * <p>Each row is sent inside its own span whose parent is the trace context stored in the row
 * (ADR 0021), so the Kafka send and the consumer join the trace of the request that wrote it. A
 * row without a context is sent in a new trace. Tracing never holds a row back: if the context
 * cannot be restored, the row is sent without it.
 */
@Component
public class OutboxRelay {

    private static final Logger log = LoggerFactory.getLogger(OutboxRelay.class);

    /** Key of the transaction-scoped advisory lock that makes this the single active relay. */
    private static final int LOCK_CLASS = 4242;
    private static final int LOCK_RELAY = 1;

    /** Errors that will fail the same way on every retry: the row is parked, not retried. */
    private static final List<Class<? extends Throwable>> POISON = List.of(
            RecordTooLargeException.class,
            RecordBatchTooLargeException.class,
            SerializationException.class,
            InvalidTopicException.class,
            // The stored payload is not valid JSON: it will never become an envelope.
            JacksonException.class);

    private record Pending(long id, UUID eventId, String aggregateType, UUID aggregateId, String eventType,
            int schemaVersion, String payload, Instant occurredAt, String traceparent) {
    }

    private final TransactionTemplate transaction;
    private final JdbcClient jdbc;
    private final KafkaTemplate<String, LedgerEvent> kafka;
    private final JsonMapper json;
    private final String topic;
    private final int batchSize;
    private final Duration sendTimeout;
    private final Duration retention;
    private final Tracer tracer;
    private final Propagator propagator;
    private volatile Runnable afterSendHook = () -> { };

    OutboxRelay(
            PlatformTransactionManager transactionManager,
            JdbcClient jdbc,
            KafkaTemplate<String, LedgerEvent> kafka,
            JsonMapper json,
            Tracer tracer,
            Propagator propagator,
            @Value("${ledger.outbox.topic}") String topic,
            @Value("${ledger.outbox.relay.batch-size:500}") int batchSize,
            @Value("${ledger.outbox.relay.send-timeout-ms:10000}") long sendTimeoutMs,
            @Value("${ledger.outbox.retention-days:7}") int retentionDays) {
        this.transaction = new TransactionTemplate(transactionManager);
        this.jdbc = jdbc;
        this.kafka = kafka;
        this.json = json;
        this.topic = topic;
        this.batchSize = batchSize;
        this.sendTimeout = Duration.ofMillis(sendTimeoutMs);
        this.retention = Duration.ofDays(retentionDays);
        this.tracer = tracer;
        this.propagator = propagator;
    }

    /** Drains the backlog batch by batch; a failure ends this tick and the next one retries. */
    @Scheduled(fixedDelayString = "${ledger.outbox.relay.interval-ms:100}")
    void relay() {
        try {
            while (relayBatch() == batchSize) {
                // a full batch means more may be waiting
            }
        } catch (RuntimeException e) {
            log.warn("outbox relay batch failed, rows stay pending for the next tick: {}", e.toString());
        }
    }

    /**
     * Publishes one batch. Returns the number of rows claimed, 0 when nothing is pending or
     * another relay holds the lock.
     */
    public int relayBatch() {
        Integer claimed = transaction.execute(status -> {
            boolean locked = jdbc.sql("select pg_try_advisory_xact_lock(:class, :relay)")
                    .param("class", LOCK_CLASS)
                    .param("relay", LOCK_RELAY)
                    .query(Boolean.class)
                    .single();
            if (!locked) {
                return 0;
            }
            List<Pending> batch = claim();
            if (!batch.isEmpty()) {
                publish(batch);
            }
            return batch.size();
        });
        return Objects.requireNonNullElse(claimed, 0);
    }

    private List<Pending> claim() {
        return jdbc.sql("""
                        select id, event_id, aggregate_type, aggregate_id, event_type, schema_version,
                               payload::text as payload, occurred_at, traceparent
                        from outbox_events
                        where published_at is null and parked_at is null
                        order by id
                        limit :limit
                        for update skip locked
                        """)
                .param("limit", batchSize)
                .query((rs, row) -> new Pending(
                        rs.getLong("id"),
                        rs.getObject("event_id", UUID.class),
                        rs.getString("aggregate_type"),
                        rs.getObject("aggregate_id", UUID.class),
                        rs.getString("event_type"),
                        rs.getInt("schema_version"),
                        rs.getString("payload"),
                        rs.getObject("occurred_at", OffsetDateTime.class).toInstant(),
                        rs.getString("traceparent")))
                .list();
    }

    private void publish(List<Pending> batch) {
        // Send everything first so the producer can batch, then wait in sequence order. A send
        // that failed on the spot without being poison (no broker metadata within max.block.ms)
        // stops the sending: each further send would block as long again.
        List<CompletableFuture<SendResult<String, LedgerEvent>>> sends = new ArrayList<>();
        for (Pending event : batch) {
            CompletableFuture<SendResult<String, LedgerEvent>> send = send(event);
            sends.add(send);
            if (send.isCompletedExceptionally() && poison(send.exceptionNow()) == null) {
                break;
            }
        }
        List<Long> published = new ArrayList<>();
        for (int i = 0; i < sends.size(); i++) {
            Pending event = batch.get(i);
            try {
                sends.get(i).get(sendTimeout.toMillis(), TimeUnit.MILLISECONDS);
                published.add(event.id());
            } catch (ExecutionException | RuntimeException e) {
                Throwable cause = e instanceof ExecutionException ? e.getCause() : e;
                Throwable poison = poison(cause);
                if (poison != null) {
                    park(event, poison);
                } else {
                    failed(batch.subList(i, batch.size()), cause);
                    break;
                }
            } catch (TimeoutException e) {
                failed(batch.subList(i, batch.size()), e);
                break;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                failed(batch.subList(i, batch.size()), e);
                break;
            }
        }
        afterSendHook.run();
        if (!published.isEmpty()) {
            jdbc.sql("""
                            update outbox_events set published_at = now(), attempts = attempts + 1
                            where id in (:ids)
                            """)
                    .param("ids", published)
                    .update();
        }
    }

    private CompletableFuture<SendResult<String, LedgerEvent>> send(Pending event) {
        Span span = relaySpan(event);
        try (Tracer.SpanInScope scope = tracer.withSpan(span)) {
            // The Kafka template's observation takes the span in scope as its parent and writes
            // the traceparent header from its own producer span.
            LedgerEvent envelope = new LedgerEvent(event.eventId(), event.eventType(), event.schemaVersion(),
                    event.occurredAt(), event.aggregateType(), event.aggregateId(), event.id(),
                    json.readTree(event.payload()));
            return kafka.send(topic, event.aggregateId().toString(), envelope);
        } catch (RuntimeException e) {
            if (span != null) {
                span.error(e);
            }
            return CompletableFuture.failedFuture(e);
        } finally {
            if (span != null) {
                span.end();
            }
        }
    }

    /**
     * A span for sending one row: child of the row's stored context, or a new root when the row
     * has none. Null when tracing fails, so the row goes out untraced instead of not at all.
     */
    private Span relaySpan(Pending event) {
        try {
            Span.Builder builder = event.traceparent() == null
                    ? tracer.spanBuilder().setNoParent()
                    : propagator.extract(Map.of(OutboxWriter.TRACEPARENT, event.traceparent()), Map::get);
            return builder.name("outbox relay")
                    .tag("ledger.outbox.sequence", String.valueOf(event.id()))
                    .start();
        } catch (RuntimeException e) {
            log.debug("outbox event {} sent without trace context: {}", event.eventId(), e.toString());
            return null;
        }
    }

    /** The non-retriable Kafka error in the cause chain, or null when the failure may pass on retry. */
    private static Throwable poison(Throwable error) {
        for (Throwable t = error; t != null; t = t.getCause()) {
            Throwable current = t;
            if (POISON.stream().anyMatch(type -> type.isInstance(current))) {
                return current;
            }
        }
        return null;
    }

    private void park(Pending event, Throwable cause) {
        log.error("outbox event {} (sequence {}) parked, Kafka rejected it for good: {}",
                event.eventId(), event.id(), cause.toString());
        jdbc.sql("""
                        update outbox_events set parked_at = now(), attempts = attempts + 1, last_error = :error
                        where id = :id
                        """)
                .param("error", cause.toString())
                .param("id", event.id())
                .update();
    }

    private void failed(List<Pending> unsent, Throwable error) {
        Throwable cause = error;
        while (cause.getCause() != null) {
            cause = cause.getCause(); // the Kafka error, not Spring's wrapper
        }
        log.warn("outbox relay could not publish {} events, retrying on the next tick: {}",
                unsent.size(), cause.toString());
        jdbc.sql("""
                        update outbox_events set attempts = attempts + 1, last_error = :error
                        where id in (:ids)
                        """)
                .param("error", cause.toString())
                .param("ids", unsent.stream().map(Pending::id).toList())
                .update();
    }

    /** Deletes rows published before {@code cutoff}. Pending and parked rows are kept. */
    public int deletePublishedBefore(Instant cutoff) {
        return jdbc.sql("delete from outbox_events where published_at < :cutoff")
                .param("cutoff", cutoff.atOffset(ZoneOffset.UTC))
                .update();
    }

    @Scheduled(fixedDelayString = "${ledger.outbox.cleanup.interval-ms:3600000}")
    void cleanup() {
        int deleted = deletePublishedBefore(Instant.now().minus(retention));
        if (deleted > 0) {
            log.info("outbox cleanup deleted {} events published more than {} ago", deleted, retention);
        }
    }

    /**
     * Test seam (ADR 0019): runs after the batch was sent and acknowledged, before the rows are
     * marked published. A no-op unless a test sets it.
     */
    public void setAfterSendHook(Runnable hook) {
        this.afterSendHook = hook == null ? () -> { } : hook;
    }
}
