package tech.lucassantana.ledger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.zaxxer.hikari.HikariDataSource;
import io.micrometer.core.instrument.MeterRegistry;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.system.CapturedOutput;
import org.springframework.boot.test.system.OutputCaptureExtension;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.context.WebApplicationContext;
import tech.lucassantana.ledger.account.Account;
import tech.lucassantana.ledger.account.AccountRepository;
import tech.lucassantana.ledger.money.Money;
import tech.lucassantana.ledger.outbox.LedgerEvent;
import tech.lucassantana.ledger.outbox.TransferCommitted;
import tech.lucassantana.ledger.transfer.TransferService;
import tools.jackson.databind.json.JsonMapper;

/**
 * Spec {@code docs/specs/2026-11-18-pool-bounds}, acceptance 2 and 4: with a 1 s pool timeout and
 * every connection taken, a request answers 503 with {@code Retry-After} and the overloaded type,
 * the relay logs and retries on the next tick, and the statement consumer retries without limit
 * and dead-letters nothing. Once the pool frees, both catch up.
 */
@SpringBootTest(properties = {
        "spring.datasource.hikari.connection-timeout=1000",
        "ledger.reconciliation.interval-ms=3600000"})
@Import(TestcontainersConfiguration.class)
@ExtendWith(OutputCaptureExtension.class)
class PoolTimeoutTest {

    /** Longer than the consumer's poison budget (4 tries of 1 s plus 3 back-offs of 500 ms). */
    private static final Duration EXHAUSTED = Duration.ofSeconds(8);
    private static final Duration WAIT = Duration.ofSeconds(30);

    @Autowired
    private WebApplicationContext context;
    @Autowired
    private DataSource dataSource;
    @Autowired
    private JdbcClient jdbc;
    @Autowired
    private TransferService service;
    @Autowired
    private AccountRepository accounts;
    @Autowired
    private KafkaTemplate<String, LedgerEvent> kafka;
    @Autowired
    private JsonMapper json;
    @Autowired
    private MeterRegistry meters;
    @Value("${ledger.outbox.topic}")
    private String topic;

    @Test
    void anExhaustedPoolAnswers503AndTheRelayAndConsumerWaitItOut(CapturedOutput output) throws Exception {
        MockMvc mvc = MockMvcBuilders.webAppContextSetup(context).build();
        UUID a = openWith(10_00);
        UUID b = openWith(0);
        UUID transferId = service.transfer(key(), a, b, new Money(1_00)).transfer().getId();
        awaitLines(transferId, 2);
        double deadLettered = meters.counter("ledger.statement.dead-lettered").count();
        UUID extra = UUID.randomUUID();

        List<Connection> held = exhaust();
        try {
            // The transfer goes back to pending for the relay, and a new event reaches the consumer,
            // both while neither can get a connection.
            try (PreparedStatement update = held.getFirst().prepareStatement(
                    "update outbox_events set published_at = null where aggregate_id = ?")) {
                update.setObject(1, transferId);
                assertThat(update.executeUpdate()).isEqualTo(1);
            }
            send(extra, a, b, 50);

            mvc.perform(get("/accounts/" + a))
                    .andExpect(status().isServiceUnavailable())
                    .andExpect(header().string("Retry-After", "1"))
                    .andExpect(jsonPath("$.type").value("/problems/overloaded"));

            Thread.sleep(EXHAUSTED);
            assertThat(count(held.getFirst(),
                    "select count(*) from outbox_events where aggregate_id = ? and published_at is null", transferId))
                    .isEqualTo(1);
            assertThat(count(held.getFirst(),
                    "select count(*) from statement.statement_lines where transfer_id = ?", extra)).isZero();
        } finally {
            for (Connection connection : held) {
                connection.close();
            }
        }

        assertThat(output.getAll()).contains("outbox relay batch failed, rows stay pending for the next tick");
        await().atMost(WAIT).until(() -> jdbc.sql(
                        "select count(*) from outbox_events where aggregate_id = ? and published_at is null")
                .param(transferId).query(Long.class).single() == 0);
        awaitLines(extra, 2);
        assertThat(meters.counter("ledger.statement.dead-lettered").count()).isEqualTo(deadLettered);
        mvc.perform(get("/accounts/" + a)).andExpect(status().isOk());
    }

    /** Takes every connection of the pool; the relay and consumer release theirs within a second. */
    private List<Connection> exhaust() throws SQLException {
        HikariDataSource pool = dataSource.unwrap(HikariDataSource.class);
        List<Connection> held = new ArrayList<>();
        try {
            for (int i = 0; i < pool.getMaximumPoolSize(); i++) {
                held.add(pool.getConnection());
            }
        } catch (SQLException e) {
            for (Connection connection : held) {
                connection.close();
            }
            throw e;
        }
        return held;
    }

    private static long count(Connection connection, String sql, UUID id) throws SQLException {
        try (PreparedStatement query = connection.prepareStatement(sql)) {
            query.setObject(1, id);
            try (ResultSet rs = query.executeQuery()) {
                rs.next();
                return rs.getLong(1);
            }
        }
    }

    private void send(UUID transferId, UUID from, UUID to, long cents) {
        var payload = new TransferCommitted(transferId, from, to, cents);
        kafka.send(topic, transferId.toString(), new LedgerEvent(UUID.randomUUID(), TransferCommitted.EVENT_TYPE,
                TransferCommitted.SCHEMA_VERSION, Instant.now(), TransferCommitted.AGGREGATE_TYPE, transferId,
                Long.MAX_VALUE, json.valueToTree(payload))).join();
    }

    private void awaitLines(UUID transferId, long lines) {
        await().atMost(WAIT).until(() -> jdbc.sql(
                        "select count(*) from statement.statement_lines where transfer_id = ?")
                .param(transferId).query(Long.class).single() == lines);
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
