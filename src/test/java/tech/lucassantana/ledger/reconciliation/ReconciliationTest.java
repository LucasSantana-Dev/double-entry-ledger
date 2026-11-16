package tech.lucassantana.ledger.reconciliation;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.micrometer.core.instrument.MeterRegistry;
import java.sql.Connection;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.test.context.TestPropertySource;
import tech.lucassantana.ledger.TestcontainersConfiguration;
import tech.lucassantana.ledger.account.Account;
import tech.lucassantana.ledger.account.AccountRepository;
import tech.lucassantana.ledger.money.Money;
import tech.lucassantana.ledger.reconciliation.ReconciliationReport.AccountDifference;
import tech.lucassantana.ledger.transfer.TransferService;

/**
 * Reconciliation finds what it claims to find. Runs in its own context and database, because it
 * tampers with the statement on purpose.
 */
@SpringBootTest
@Import(TestcontainersConfiguration.class)
@TestPropertySource(properties = "ledger.reconciliation.interval-ms=3600000")
class ReconciliationTest {

    @Autowired
    private TransferService service;
    @Autowired
    private AccountRepository accounts;
    @Autowired
    private Reconciliation reconciliation;
    @Autowired
    private JdbcClient jdbc;
    @Autowired
    private DataSource dataSource;
    @Autowired
    private MeterRegistry meters;

    @Test
    void reportsNothingWhenTheStatementMatchesAndEveryKindOfDifferenceWhenItDoesNot() throws Exception {
        UUID a = openWith(100_00);
        UUID b = openWith(0);
        UUID c = openWith(0);
        List<UUID> transfers = new ArrayList<>();
        for (int i = 0; i < 6; i++) {
            transfers.add(service.transfer(key(), a, i % 2 == 0 ? b : c, new Money(1_00)).transfer().getId());
        }
        long ledgerTransfers = jdbc.sql("select count(*) from transfers").query(Long.class).single();
        await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> assertThat(
                jdbc.sql("select count(*) from statement.statement_lines").query(Long.class).single())
                .isEqualTo(2 * ledgerTransfers));

        ReconciliationReport clean = reconciliation.reconcileAll(Instant.now());
        assertThat(clean.isClean()).as("report %s", clean).isTrue();
        assertThat(clean.ledgerTransfers()).isEqualTo(ledgerTransfers);
        assertThat(clean.statementTransfers()).isEqualTo(ledgerTransfers);
        assertThat(differencesGauge()).as("ledger.reconciliation.differences after a clean run").isZero();

        // Events still in the outbox are not missing: pending is on its way, parked is reported apart.
        UUID waiting;
        UUID rejected;
        try (Connection other = dataSource.getConnection(); Statement sql = other.createStatement()) {
            sql.execute("select pg_advisory_lock(4242, 1)"); // the relay is held, as during a broker outage
            waiting = service.transfer(key(), a, b, new Money(1_00)).transfer().getId();
            ReconciliationReport outage = reconciliation.reconcileAll(Instant.now());
            assertThat(outage.isClean()).as("report %s", outage).isTrue();
            assertThat(outage.pendingInOutbox()).containsExactly(waiting);
            assertThat(outage.missingFromStatement()).isEmpty();

            rejected = service.transfer(key(), a, c, new Money(1_00)).transfer().getId();
            jdbc.sql("update outbox_events set parked_at = now(), last_error = 'rejected' where aggregate_id = ?")
                    .param(rejected).update();
            ReconciliationReport poisoned = reconciliation.reconcileAll(Instant.now());
            assertThat(poisoned.isClean()).isFalse();
            assertThat(poisoned.parkedInOutbox()).containsExactly(rejected);
            assertThat(poisoned.missingFromStatement()).isEmpty();
            assertThat(poisoned.windows()).isEmpty();
            sql.execute("select pg_advisory_unlock(4242, 1)");
        }
        await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> assertThat(
                jdbc.sql("select count(*) from statement.statement_lines where transfer_id = ?")
                        .param(waiting).query(Long.class).single()).isEqualTo(2));

        // A lost event: both lines of one transfer are gone.
        UUID lost = transfers.get(0);
        jdbc.sql("delete from statement.statement_lines where transfer_id = ?").param(lost).update();
        // A corrupted event: the credit of another transfer is wrong.
        UUID corrupted = transfers.get(1);
        jdbc.sql("update statement.statement_lines set amount_cents = 200 where transfer_id = ? and amount_cents > 0")
                .param(corrupted).update();
        // A line for a transfer the ledger never had.
        UUID phantom = UUID.randomUUID();
        jdbc.sql("""
                        insert into statement.statement_lines
                            (account_id, transfer_id, event_id, sequence, amount_cents, occurred_at)
                        values (?, ?, ?, 0, 500, now())
                        """)
                .params(b, phantom, UUID.randomUUID()).update();

        Instant tampered = Instant.now();
        ReconciliationReport report = reconciliation.reconcileAll(tampered);

        assertThat(report.isClean()).isFalse();
        assertThat(report.missingFromStatement()).containsExactly(lost);
        assertThat(report.malformedInStatement()).containsExactlyInAnyOrder(corrupted, phantom);
        assertThat(report.unknownInStatement()).containsExactly(phantom);
        assertThat(report.windows()).isNotEmpty();
        // Statement minus ledger, per account, summed over the differing windows. Lost: a misses its
        // -1.00 and b its +1.00. Corrupted: c got +2.00 instead of +1.00. Phantom: b got +5.00.
        assertThat(report.accounts().stream().collect(Collectors.groupingBy(AccountDifference::accountId,
                Collectors.summingLong(d -> d.statementCents() - d.ledgerCents()))))
                .isEqualTo(Map.of(a, 1_00L, b, -1_00L + 5_00L, c, 1_00L));
        assertThat(report.parkedInOutbox()).containsExactly(rejected);
        assertThat(differencesGauge()).as("ledger.reconciliation.differences")
                .isEqualTo(report.differences()).isPositive();

        // The scheduled check looks at a sliding window: differences before it are out of scope.
        ReconciliationReport later = reconciliation.reconcile(tampered.plusMillis(1), Instant.now());
        assertThat(later.isClean()).as("report %s", later).isTrue();
        assertThat(later.ledgerTransfers()).isZero();
        assertThat(differencesGauge()).as("the gauge follows the last run").isZero();
    }

    /** Phase 4 criterion 6 (ADR 0022): the gauge reports the last run, whichever call made it. */
    private long differencesGauge() {
        return (long) meters.get("ledger.reconciliation.differences").gauge().value();
    }

    private UUID openWith(long cents) {
        UUID id = accounts.save(Account.openCustomer()).getId();
        if (cents > 0) {
            service.deposit(key(), id, new Money(cents));
        }
        return id;
    }

    private static String key() {
        return UUID.randomUUID().toString();
    }
}
