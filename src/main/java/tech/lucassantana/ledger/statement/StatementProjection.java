package tech.lucassantana.ledger.statement;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.ZoneOffset;
import java.util.UUID;
import java.util.function.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * Builds the account statement from {@code TransferCommitted} events (ADR 0017). Delivery is at
 * least once, so each event is applied in one database transaction together with its event id in
 * {@code processed_events}; a second delivery finds the id and changes nothing. The offset is
 * committed by the listener container after this method returns, so a crash in between redelivers
 * an event that was already applied, which the same check absorbs.
 */
@Component
public class StatementProjection {

    private static final Logger log = LoggerFactory.getLogger(StatementProjection.class);
    private static final String TRANSFER_COMMITTED = "TransferCommitted";

    private final TransactionTemplate transaction;
    private final JdbcClient jdbc;
    private final Counter applied;
    private final Counter duplicates;
    private volatile Consumer<TransferEvent> afterApplyHook = event -> { };

    StatementProjection(PlatformTransactionManager transactionManager, JdbcClient jdbc, MeterRegistry meters) {
        this.transaction = new TransactionTemplate(transactionManager);
        this.jdbc = jdbc;
        this.applied = meters.counter("ledger.statement.applied");
        this.duplicates = meters.counter("ledger.statement.duplicates");
    }

    @KafkaListener(topics = "${ledger.outbox.topic}", groupId = "${ledger.statement.group-id:ledger-statement}")
    void on(TransferEvent event) {
        if (!TRANSFER_COMMITTED.equals(event.eventType())) {
            log.warn("statement ignores event {} of unknown type {}", event.eventId(), event.eventType());
            return;
        }
        if (Boolean.TRUE.equals(transaction.execute(status -> apply(event)))) {
            applied.increment();
        } else {
            duplicates.increment();
            log.info("statement skipped duplicate event {} (sequence {})", event.eventId(), event.sequence());
        }
        afterApplyHook.accept(event);
    }

    /** Returns false when the event was already applied. */
    private boolean apply(TransferEvent event) {
        int fresh = jdbc.sql("""
                        insert into statement.processed_events (event_id) values (?)
                        on conflict (event_id) do nothing
                        """)
                .param(event.eventId())
                .update();
        if (fresh == 0) {
            return false;
        }
        TransferEvent.Payload transfer = event.payload();
        line(event, transfer.fromAccountId(), -transfer.amountCents());
        line(event, transfer.toAccountId(), transfer.amountCents());
        return true;
    }

    private void line(TransferEvent event, UUID accountId, long amountCents) {
        jdbc.sql("""
                        insert into statement.statement_lines
                            (account_id, transfer_id, event_id, sequence, amount_cents, occurred_at)
                        values (?, ?, ?, ?, ?, ?)
                        """)
                .params(accountId, event.payload().transferId(), event.eventId(), event.sequence(), amountCents,
                        event.occurredAt().atOffset(ZoneOffset.UTC))
                .update();
    }

    /**
     * Test seam (ADR 0019): runs after the event's database transaction committed and before the
     * listener returns, that is before the offset commit. A no-op unless a test sets it.
     */
    public void setAfterApplyHook(Consumer<TransferEvent> hook) {
        this.afterApplyHook = hook == null ? event -> { } : hook;
    }
}
