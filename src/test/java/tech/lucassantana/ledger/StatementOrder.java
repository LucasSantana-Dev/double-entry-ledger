package tech.lucassantana.ledger;

import org.springframework.jdbc.core.simple.JdbcClient;

/**
 * Phase 3 invariant 5, checked against PostgreSQL's own record of commit order: the test
 * containers run with {@code track_commit_timestamp=on}, so {@code pg_xact_commit_timestamp(xmin)}
 * of a transfer row is the instant its transaction committed. Per account, the statement lines
 * read in an order must never go back in commit time, and a customer's running balance must
 * never dip below zero.
 */
public final class StatementOrder {

    private StatementOrder() {
    }

    /** Lines that break commit order when each account's statement is read by ledger sequence. */
    public static long violationsBySequence(JdbcClient jdbc) {
        return violations(jdbc, "l.sequence");
    }

    /**
     * Same check with lines read in the order of {@code orderBy}. Ordering by something unrelated
     * to commit order must produce violations, which shows the check is not vacuous.
     */
    public static long violations(JdbcClient jdbc, String orderBy) {
        return jdbc.sql("""
                        select count(*) from (
                            select a.kind,
                                   sum(l.amount_cents) over w as running,
                                   pg_xact_commit_timestamp(t.xmin) as committed,
                                   lag(pg_xact_commit_timestamp(t.xmin)) over w as previous
                            from statement.statement_lines l
                            join accounts a on a.id = l.account_id
                            join transfers t on t.id = l.transfer_id
                            window w as (partition by l.account_id order by %s)
                        ) ordered
                        where (ordered.kind = 'CUSTOMER' and ordered.running < 0)
                           or ordered.committed < ordered.previous
                           or ordered.committed is null
                        """.formatted(orderBy))
                .query(Long.class)
                .single();
    }
}
