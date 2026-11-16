package tech.lucassantana.ledger.reconciliation;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
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

    @Test
    void reportsNothingWhenTheStatementMatchesAndEveryKindOfDifferenceWhenItDoesNot() {
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

        ReconciliationReport clean = reconciliation.reconcile(Instant.now());
        assertThat(clean.isClean()).as("report %s", clean).isTrue();
        assertThat(clean.ledgerTransfers()).isEqualTo(ledgerTransfers);
        assertThat(clean.statementTransfers()).isEqualTo(ledgerTransfers);

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

        ReconciliationReport report = reconciliation.reconcile(Instant.now());

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
