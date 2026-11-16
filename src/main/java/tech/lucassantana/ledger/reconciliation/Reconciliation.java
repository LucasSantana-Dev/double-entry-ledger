package tech.lucassantana.ledger.reconciliation;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionTemplate;
import tech.lucassantana.ledger.reconciliation.ReconciliationReport.AccountDifference;
import tech.lucassantana.ledger.reconciliation.ReconciliationReport.WindowDifference;

/**
 * Compares the ledger with the statement projection (ADR 0017): first transfer by transfer, then
 * count and credited sum per hour window, then per account inside the windows that differ.
 *
 * <p>The schedule checks a sliding window ending at the maximum relay delay (default the last
 * 24 hours before "one minute ago"), so its cost does not grow with history. A transfer whose
 * outbox row is still unpublished is not a difference: it is reported apart, as pending, or as
 * parked when Kafka rejected it for good (ADR 0016), and is left out of every comparison on both
 * sides. A broker outage therefore shows up as a growing pending list, not as false differences.
 */
@Component
public class Reconciliation {

    private static final Logger log = LoggerFactory.getLogger(Reconciliation.class);

    /** Transfer {@code t} still has an unpublished outbox row (pending or parked). */
    private static final String IN_OUTBOX = """
            exists (select 1 from outbox_events o where o.aggregate_id = %s and o.published_at is null)""";

    private final TransactionTemplate snapshot;
    private final JdbcClient jdbc;
    private final Duration lag;
    private final Duration window;

    Reconciliation(PlatformTransactionManager transactionManager, JdbcClient jdbc,
            @Value("${ledger.reconciliation.lag-ms:60000}") long lagMs,
            @Value("${ledger.reconciliation.window-hours:24}") long windowHours) {
        // Every query of one report reads the same snapshot, so a transfer committing midway
        // cannot show up on one side of the comparison only.
        this.snapshot = new TransactionTemplate(transactionManager);
        this.snapshot.setIsolationLevel(TransactionDefinition.ISOLATION_REPEATABLE_READ);
        this.snapshot.setReadOnly(true);
        this.jdbc = jdbc;
        this.lag = Duration.ofMillis(lagMs);
        this.window = Duration.ofHours(windowHours);
    }

    @Scheduled(initialDelayString = "${ledger.reconciliation.interval-ms:60000}",
            fixedDelayString = "${ledger.reconciliation.interval-ms:60000}")
    void scheduled() {
        Instant cutoff = Instant.now().minus(lag);
        reconcile(cutoff.minus(window), cutoff);
    }

    /** The whole history before {@code cutoff}: for tests and manual checks, not for the schedule. */
    public ReconciliationReport reconcileAll(Instant cutoff) {
        return reconcile(Instant.EPOCH, cutoff);
    }

    /** Compares what happened in {@code [from, cutoff)}, logs one line and returns the report. */
    public ReconciliationReport reconcile(Instant from, Instant cutoff) {
        ReconciliationReport report = snapshot.execute(status -> compare(from, cutoff));
        if (report.isClean()) {
            log.info("reconciliation clean from {} to {}: {} transfers in ledger and statement, {} pending in outbox",
                    from, cutoff, report.ledgerTransfers(), report.pendingInOutbox().size());
        } else {
            log.warn("reconciliation found differences from {} to {}: ledger {} transfers, statement {}; "
                            + "{} missing, {} malformed, {} unknown, {} parked, {} windows and {} accounts differ",
                    from, cutoff, report.ledgerTransfers(), report.statementTransfers(),
                    report.missingFromStatement().size(), report.malformedInStatement().size(),
                    report.unknownInStatement().size(), report.parkedInOutbox().size(),
                    report.windows().size(), report.accounts().size());
        }
        return report;
    }

    private ReconciliationReport compare(Instant from, Instant cutoff) {
        OffsetDateTime start = from.atOffset(ZoneOffset.UTC);
        OffsetDateTime end = cutoff.atOffset(ZoneOffset.UTC);
        List<UUID> pending = transfers("""
                select t.id from transfers t join outbox_events o on o.aggregate_id = t.id
                where t.created_at >= :start and t.created_at < :end
                  and o.published_at is null and o.parked_at is null
                order by t.created_at
                """, start, end);
        List<UUID> parked = transfers("""
                select t.id from transfers t join outbox_events o on o.aggregate_id = t.id
                where t.created_at >= :start and t.created_at < :end and o.parked_at is not null
                order by t.created_at
                """, start, end);
        long ledgerTransfers = jdbc.sql("""
                        select count(*) from transfers t
                        where t.created_at >= :start and t.created_at < :end and not %s
                        """.formatted(IN_OUTBOX.formatted("t.id")))
                .param("start", start).param("end", end).query(Long.class).single();
        long statementTransfers = jdbc.sql("""
                        select count(distinct l.transfer_id) from statement.statement_lines l
                        where l.occurred_at >= :start and l.occurred_at < :end and not %s
                        """.formatted(IN_OUTBOX.formatted("l.transfer_id")))
                .param("start", start).param("end", end).query(Long.class).single();
        List<UUID> missing = transfers("""
                select t.id from transfers t
                where t.created_at >= :start and t.created_at < :end and not %s
                  and not exists (select 1 from statement.statement_lines l where l.transfer_id = t.id)
                order by t.created_at
                """.formatted(IN_OUTBOX.formatted("t.id")), start, end);
        List<UUID> malformed = transfers("""
                select l.transfer_id from statement.statement_lines l
                where l.occurred_at >= :start and l.occurred_at < :end and not %s
                group by l.transfer_id having count(*) <> 2 or sum(l.amount_cents) <> 0
                order by l.transfer_id
                """.formatted(IN_OUTBOX.formatted("l.transfer_id")), start, end);
        List<UUID> unknown = transfers("""
                select distinct l.transfer_id from statement.statement_lines l
                where l.occurred_at >= :start and l.occurred_at < :end
                  and not exists (select 1 from transfers t where t.id = l.transfer_id)
                order by l.transfer_id
                """, start, end);
        List<WindowDifference> windows = windows(start, end);
        List<AccountDifference> accounts = new ArrayList<>();
        windows.forEach(w -> accounts.addAll(accounts(w.window(), start, end)));
        return new ReconciliationReport(from, cutoff, ledgerTransfers, statementTransfers, pending, parked, missing,
                malformed, unknown, windows, accounts);
    }

    private List<UUID> transfers(String sql, OffsetDateTime start, OffsetDateTime end) {
        return jdbc.sql(sql).param("start", start).param("end", end).query(UUID.class).list();
    }

    private List<WindowDifference> windows(OffsetDateTime start, OffsetDateTime end) {
        return jdbc.sql("""
                        with ledger as (
                            select date_trunc('hour', t.created_at) as w, count(*) as n, sum(t.amount_cents) as cents
                            from transfers t
                            where t.created_at >= :start and t.created_at < :end and not %s
                            group by 1
                        ), projected as (
                            select date_trunc('hour', l.occurred_at) as w, count(distinct l.transfer_id) as n,
                                   coalesce(sum(l.amount_cents) filter (where l.amount_cents > 0), 0) as cents
                            from statement.statement_lines l
                            where l.occurred_at >= :start and l.occurred_at < :end and not %s
                            group by 1
                        )
                        select coalesce(l.w, p.w) as w,
                               coalesce(l.n, 0) as ledger_n, coalesce(p.n, 0) as statement_n,
                               coalesce(l.cents, 0) as ledger_cents, coalesce(p.cents, 0) as statement_cents
                        from ledger l full outer join projected p on p.w = l.w
                        where coalesce(l.n, 0) <> coalesce(p.n, 0) or coalesce(l.cents, 0) <> coalesce(p.cents, 0)
                        order by 1
                        """.formatted(IN_OUTBOX.formatted("t.id"), IN_OUTBOX.formatted("l.transfer_id")))
                .param("start", start)
                .param("end", end)
                .query((rs, row) -> new WindowDifference(instant(rs, "w"), rs.getLong("ledger_n"),
                        rs.getLong("statement_n"), rs.getLong("ledger_cents"), rs.getLong("statement_cents")))
                .list();
    }

    private List<AccountDifference> accounts(Instant window, OffsetDateTime start, OffsetDateTime end) {
        return jdbc.sql("""
                        with ledger as (
                            select e.account_id, sum(e.amount_cents) as cents from entries e
                            where e.created_at >= :start and e.created_at < :end
                              and date_trunc('hour', e.created_at) = :window and not %s
                            group by e.account_id
                        ), projected as (
                            select l.account_id, sum(l.amount_cents) as cents from statement.statement_lines l
                            where l.occurred_at >= :start and l.occurred_at < :end
                              and date_trunc('hour', l.occurred_at) = :window and not %s
                            group by l.account_id
                        )
                        select coalesce(l.account_id, p.account_id) as account_id,
                               coalesce(l.cents, 0) as ledger_cents, coalesce(p.cents, 0) as statement_cents
                        from ledger l full outer join projected p on p.account_id = l.account_id
                        where coalesce(l.cents, 0) <> coalesce(p.cents, 0)
                        order by 1
                        """.formatted(IN_OUTBOX.formatted("e.transfer_id"), IN_OUTBOX.formatted("l.transfer_id")))
                .param("start", start)
                .param("end", end)
                .param("window", window.atOffset(ZoneOffset.UTC))
                .query((rs, row) -> new AccountDifference(window, rs.getObject("account_id", UUID.class),
                        rs.getLong("ledger_cents"), rs.getLong("statement_cents")))
                .list();
    }

    private static Instant instant(ResultSet rs, String column) throws SQLException {
        return rs.getObject(column, OffsetDateTime.class).toInstant();
    }
}
