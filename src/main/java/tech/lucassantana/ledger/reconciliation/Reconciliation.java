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
 * count and credited sum per hour window, then per account inside the windows that differ. Only
 * transfers older than the maximum relay delay are compared, so events still on their way are
 * not reported.
 */
@Component
public class Reconciliation {

    private static final Logger log = LoggerFactory.getLogger(Reconciliation.class);

    private final TransactionTemplate snapshot;
    private final JdbcClient jdbc;
    private final Duration lag;

    Reconciliation(PlatformTransactionManager transactionManager, JdbcClient jdbc,
            @Value("${ledger.reconciliation.lag-ms:60000}") long lagMs) {
        // Every query of one report reads the same snapshot, so a transfer committing midway
        // cannot show up on one side of the comparison only.
        this.snapshot = new TransactionTemplate(transactionManager);
        this.snapshot.setIsolationLevel(TransactionDefinition.ISOLATION_REPEATABLE_READ);
        this.snapshot.setReadOnly(true);
        this.jdbc = jdbc;
        this.lag = Duration.ofMillis(lagMs);
    }

    @Scheduled(initialDelayString = "${ledger.reconciliation.interval-ms:60000}",
            fixedDelayString = "${ledger.reconciliation.interval-ms:60000}")
    void scheduled() {
        reconcile(Instant.now().minus(lag));
    }

    /** Compares everything that happened before {@code cutoff}, logs one line and returns the report. */
    public ReconciliationReport reconcile(Instant cutoff) {
        ReconciliationReport report = snapshot.execute(status -> compare(cutoff));
        if (report.isClean()) {
            log.info("reconciliation clean before {}: {} transfers in ledger and statement",
                    cutoff, report.ledgerTransfers());
        } else {
            log.warn("reconciliation found differences before {}: ledger {} transfers, statement {}; {} missing, "
                            + "{} malformed, {} unknown, {} windows and {} accounts differ",
                    cutoff, report.ledgerTransfers(), report.statementTransfers(),
                    report.missingFromStatement().size(), report.malformedInStatement().size(),
                    report.unknownInStatement().size(), report.windows().size(), report.accounts().size());
        }
        return report;
    }

    private ReconciliationReport compare(Instant cutoff) {
        OffsetDateTime before = cutoff.atOffset(ZoneOffset.UTC);
        long ledgerTransfers = jdbc.sql("select count(*) from transfers where created_at < ?")
                .param(before).query(Long.class).single();
        long statementTransfers = jdbc.sql("""
                        select count(distinct transfer_id) from statement.statement_lines where occurred_at < ?
                        """)
                .param(before).query(Long.class).single();
        List<UUID> missing = ids("""
                select t.id from transfers t
                where t.created_at < ?
                  and not exists (select 1 from statement.statement_lines l where l.transfer_id = t.id)
                order by t.created_at
                """, before);
        List<UUID> malformed = ids("""
                select transfer_id from statement.statement_lines where occurred_at < ?
                group by transfer_id having count(*) <> 2 or sum(amount_cents) <> 0
                order by transfer_id
                """, before);
        List<UUID> unknown = ids("""
                select distinct l.transfer_id from statement.statement_lines l
                where l.occurred_at < ? and not exists (select 1 from transfers t where t.id = l.transfer_id)
                order by l.transfer_id
                """, before);
        List<WindowDifference> windows = windows(before);
        List<AccountDifference> accounts = new ArrayList<>();
        windows.forEach(window -> accounts.addAll(accounts(window.window(), before)));

        return new ReconciliationReport(cutoff, ledgerTransfers, statementTransfers, missing, malformed,
                unknown, windows, accounts);
    }

    private List<UUID> ids(String sql, OffsetDateTime before) {
        return jdbc.sql(sql).param(before).query(UUID.class).list();
    }

    private List<WindowDifference> windows(OffsetDateTime before) {
        return jdbc.sql("""
                        with ledger as (
                            select date_trunc('hour', created_at) as w, count(*) as n, sum(amount_cents) as cents
                            from transfers where created_at < :before group by 1
                        ), projected as (
                            select date_trunc('hour', occurred_at) as w, count(distinct transfer_id) as n,
                                   coalesce(sum(amount_cents) filter (where amount_cents > 0), 0) as cents
                            from statement.statement_lines where occurred_at < :before group by 1
                        )
                        select coalesce(l.w, p.w) as w,
                               coalesce(l.n, 0) as ledger_n, coalesce(p.n, 0) as statement_n,
                               coalesce(l.cents, 0) as ledger_cents, coalesce(p.cents, 0) as statement_cents
                        from ledger l full outer join projected p on p.w = l.w
                        where coalesce(l.n, 0) <> coalesce(p.n, 0) or coalesce(l.cents, 0) <> coalesce(p.cents, 0)
                        order by 1
                        """)
                .param("before", before)
                .query((rs, row) -> new WindowDifference(instant(rs, "w"), rs.getLong("ledger_n"),
                        rs.getLong("statement_n"), rs.getLong("ledger_cents"), rs.getLong("statement_cents")))
                .list();
    }

    private List<AccountDifference> accounts(Instant window, OffsetDateTime before) {
        return jdbc.sql("""
                        with ledger as (
                            select account_id, sum(amount_cents) as cents from entries
                            where created_at < :before and date_trunc('hour', created_at) = :window
                            group by account_id
                        ), projected as (
                            select account_id, sum(amount_cents) as cents from statement.statement_lines
                            where occurred_at < :before and date_trunc('hour', occurred_at) = :window
                            group by account_id
                        )
                        select coalesce(l.account_id, p.account_id) as account_id,
                               coalesce(l.cents, 0) as ledger_cents, coalesce(p.cents, 0) as statement_cents
                        from ledger l full outer join projected p on p.account_id = l.account_id
                        where coalesce(l.cents, 0) <> coalesce(p.cents, 0)
                        order by 1
                        """)
                .param("before", before)
                .param("window", window.atOffset(ZoneOffset.UTC))
                .query((rs, row) -> new AccountDifference(window, rs.getObject("account_id", UUID.class),
                        rs.getLong("ledger_cents"), rs.getLong("statement_cents")))
                .list();
    }

    private static Instant instant(ResultSet rs, String column) throws SQLException {
        return rs.getObject(column, OffsetDateTime.class).toInstant();
    }
}
