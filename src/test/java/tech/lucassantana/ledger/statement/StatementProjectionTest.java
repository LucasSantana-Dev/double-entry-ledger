package tech.lucassantana.ledger.statement;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.micrometer.core.instrument.MeterRegistry;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.kafka.core.KafkaTemplate;
import tech.lucassantana.ledger.TestcontainersConfiguration;
import tech.lucassantana.ledger.account.Account;
import tech.lucassantana.ledger.account.AccountRepository;
import tech.lucassantana.ledger.money.Money;
import tech.lucassantana.ledger.outbox.LedgerEvent;
import tech.lucassantana.ledger.outbox.TransferCommitted;
import tech.lucassantana.ledger.transfer.TransferService;
import tools.jackson.databind.json.JsonMapper;

/** Invariant 3 of the phase 3 spec: each event is applied once, whatever the delivery does. */
@SpringBootTest
@Import(TestcontainersConfiguration.class)
class StatementProjectionTest {

    private static final Duration WAIT = Duration.ofSeconds(30);

    @Autowired
    private TransferService service;
    @Autowired
    private AccountRepository accounts;
    @Autowired
    private StatementProjection projection;
    @Autowired
    private JdbcClient jdbc;
    @Autowired
    private MeterRegistry meters;
    @Autowired
    private KafkaTemplate<String, LedgerEvent> kafka;
    @Autowired
    private JsonMapper json;
    @Value("${ledger.outbox.topic}")
    private String topic;

    @AfterEach
    void removeHook() {
        projection.setAfterApplyHook(null);
    }

    @Test
    void appliesTheLinesOfATransfer() {
        UUID a = openWith(10_00);
        UUID b = openWith(0);
        UUID transferId = service.transfer(key(), a, b, new Money(3_00)).transfer().getId();

        awaitLines(transferId);
        assertThat(jdbc.sql("""
                        select amount_cents from statement.statement_lines where transfer_id = ? order by amount_cents
                        """)
                .param(transferId).query(Long.class).list()).containsExactly(-3_00L, 3_00L);
        assertThat(balance(a)).isEqualTo(7_00);
        assertThat(balance(b)).isEqualTo(3_00);
    }

    @Test
    void appliesAnEventOnceWhenTheRelaySendsItAgain() {
        UUID transferId = service.transfer(key(), openWith(10_00), openWith(0), new Money(1_00)).transfer().getId();
        awaitLines(transferId);
        double duplicatesBefore = duplicates();

        // Forced re-send: the row goes back to pending and the relay publishes the same event again.
        jdbc.sql("update outbox_events set published_at = null where aggregate_id = ?").param(transferId).update();

        await().atMost(WAIT).until(() -> duplicates() > duplicatesBefore);
        assertApplyOnce(transferId);
    }

    @Test
    void appliesAnEventOnceWhenTheConsumerCrashesBeforeTheOffsetCommit() {
        AtomicInteger crashes = new AtomicInteger();
        UUID a = openWith(10_00);
        UUID b = openWith(0);
        projection.setAfterApplyHook(event -> {
            // The database transaction has committed; the offset has not. Fail once, like a crash.
            if (event.payload().fromAccountId().equals(a) && crashes.getAndIncrement() == 0) {
                throw new IllegalStateException("simulated consumer crash after the statement commit");
            }
        });
        double duplicatesBefore = duplicates();

        UUID transferId = service.transfer(key(), a, b, new Money(1_00)).transfer().getId();

        await().atMost(WAIT).until(() -> duplicates() > duplicatesBefore);
        assertThat(crashes.get()).as("deliveries of the crashed event").isEqualTo(2);
        assertApplyOnce(transferId);
    }

    @Test
    void ordersAnAccountByLedgerSequenceNotByArrival() {
        UUID account = UUID.randomUUID();
        UUID other = UUID.randomUUID();
        UUID first = UUID.randomUUID();
        UUID second = UUID.randomUUID();
        long base = System.nanoTime();
        // Sequence base + 2 is delivered before base + 1, as after a retried batch.
        send(second, account, other, 4_00, base + 2);
        send(first, other, account, 5_00, base + 1);

        awaitLines(first);
        awaitLines(second);
        List<UUID> statement = jdbc.sql("""
                        select transfer_id from statement.statement_lines where account_id = ? order by sequence
                        """)
                .param(account).query(UUID.class).list();
        assertThat(statement).containsExactly(first, second);
    }

    private void send(UUID transferId, UUID from, UUID to, long cents, long sequence) {
        var payload = new TransferCommitted(transferId, from, to, cents);
        kafka.send(topic, transferId.toString(), new LedgerEvent(UUID.randomUUID(), TransferCommitted.EVENT_TYPE,
                TransferCommitted.SCHEMA_VERSION, Instant.now(), TransferCommitted.AGGREGATE_TYPE, transferId,
                sequence, json.valueToTree(payload))).join();
    }

    private void assertApplyOnce(UUID transferId) {
        assertThat(jdbc.sql("select count(*) from statement.statement_lines where transfer_id = ?")
                .param(transferId).query(Long.class).single()).isEqualTo(2);
        assertThat(jdbc.sql("""
                        select count(*) from statement.processed_events p
                        join outbox_events o on o.event_id = p.event_id
                        where o.aggregate_id = ?
                        """)
                .param(transferId).query(Long.class).single()).isEqualTo(1);
    }

    private void awaitLines(UUID transferId) {
        await().atMost(WAIT).untilAsserted(() -> assertThat(jdbc.sql(
                        "select count(*) from statement.statement_lines where transfer_id = ?")
                .param(transferId).query(Long.class).single()).isEqualTo(2));
    }

    private long balance(UUID accountId) {
        return jdbc.sql("select coalesce(sum(amount_cents), 0) from statement.statement_lines where account_id = ?")
                .param(accountId).query(Long.class).single();
    }

    private double duplicates() {
        return meters.counter("ledger.statement.duplicates").count();
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
