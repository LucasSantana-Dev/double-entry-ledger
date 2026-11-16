package tech.lucassantana.ledger.outbox;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * Outbox gauges (ADR 0022): how many rows wait for the relay, how old the oldest one is, and how
 * many Kafka rejected for good. One query per scheduled refresh feeds all three, so an export or
 * a scrape never touches the database. A failed refresh keeps the last values.
 */
@Component
public class OutboxMetrics {

    private static final Logger log = LoggerFactory.getLogger(OutboxMetrics.class);

    private record Snapshot(long pending, double oldestPendingAgeSeconds, long parked) {
    }

    private final JdbcClient jdbc;
    private volatile Snapshot last = new Snapshot(0, 0, 0);

    OutboxMetrics(JdbcClient jdbc, MeterRegistry meters) {
        this.jdbc = jdbc;
        Gauge.builder("ledger.outbox.pending", this, m -> m.last.pending())
                .description("Outbox rows not yet published to Kafka, parked rows excluded")
                .register(meters);
        Gauge.builder("ledger.outbox.oldest.pending.age", this, m -> m.last.oldestPendingAgeSeconds())
                .description("Age of the oldest pending outbox row, 0 when none is pending")
                .baseUnit("seconds")
                .register(meters);
        Gauge.builder("ledger.outbox.parked", this, m -> m.last.parked())
                .description("Outbox rows Kafka rejected for good")
                .register(meters);
    }

    // Shares Boot's single scheduler thread with the relay and the reconciliation: during a Kafka
    // outage the gauges go stale for at most one relay tick, bounded by the relay's send timeout.
    @Scheduled(fixedDelayString = "${ledger.outbox.metrics.refresh-ms:5000}")
    void scheduledRefresh() {
        try {
            refresh();
        } catch (RuntimeException e) {
            log.warn("outbox gauges not refreshed, keeping the last values: {}", e.toString());
        }
    }

    /**
     * Reads the three values in one statement. The pending partial index serves the pending count
     * and the oldest pending row, taken as the lowest id (ids follow commit order, ADR 0016), so
     * finding it reads one index entry instead of the whole backlog. The parked partial index
     * serves the parked count. Both counts still read every matching index entry, so their cost
     * grows with the backlog and the parked rows.
     *
     * <p>{@code occurred_at} is written with the application's clock and read against the
     * database's, so the age is clamped at 0: clock skew between the two never gives a negative
     * age. It reads 0 when nothing is pending.
     */
    public void refresh() {
        last = jdbc.sql("""
                        select
                            (select count(*) from outbox_events
                             where published_at is null and parked_at is null) as pending,
                            coalesce((select greatest(extract(epoch from clock_timestamp() - occurred_at), 0)
                                      from outbox_events
                                      where published_at is null and parked_at is null
                                      order by id
                                      limit 1), 0) as oldest_age,
                            (select count(*) from outbox_events where parked_at is not null) as parked
                        """)
                .query((rs, row) -> new Snapshot(rs.getLong("pending"), rs.getDouble("oldest_age"),
                        rs.getLong("parked")))
                .single();
    }
}
