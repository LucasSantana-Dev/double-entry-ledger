package tech.lucassantana.ledger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import eu.rekawek.toxiproxy.Proxy;
import eu.rekawek.toxiproxy.ToxiproxyClient;
import io.micrometer.core.instrument.MeterRegistry;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.Network;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.toxiproxy.ToxiproxyContainer;
import org.testcontainers.utility.DockerImageName;
import tech.lucassantana.ledger.account.Account;
import tech.lucassantana.ledger.account.AccountRepository;
import tech.lucassantana.ledger.money.Money;
import tech.lucassantana.ledger.outbox.OutboxRelay;
import tech.lucassantana.ledger.reconciliation.Reconciliation;
import tech.lucassantana.ledger.reconciliation.ReconciliationReport;
import tech.lucassantana.ledger.statement.StatementProjection;
import tech.lucassantana.ledger.transfer.TransferService;

/**
 * The four faults of ADR 0019, injected for real: the application reaches PostgreSQL only through
 * Toxiproxy, so cutting the proxy drops its JDBC connections mid-transaction, and the Kafka
 * container is paused through the Docker API. After each fault the system must recover on its
 * own and end with the phase 3 invariants intact.
 */
@SpringBootTest(properties = "ledger.reconciliation.interval-ms=3600000")
class FaultInjectionTest {

    private static final DockerImageName TOXIPROXY = DockerImageName.parse("ghcr.io/shopify/toxiproxy:2.12.0");
    private static final int PROXY_PORT = 8666;
    private static final int TRANSFERS = 40;
    private static final Duration RECOVERY = Duration.ofSeconds(120);

    private static final Network NETWORK = Network.newNetwork();
    private static final PostgreSQLContainer POSTGRES = new PostgreSQLContainer(TestcontainersConfiguration.POSTGRES)
            .withNetwork(NETWORK)
            .withNetworkAliases("postgres");
    private static final ToxiproxyContainer TOXIPROXY_CONTAINER = new ToxiproxyContainer(TOXIPROXY).withNetwork(NETWORK);
    private static final KafkaContainer KAFKA = new KafkaContainer(TestcontainersConfiguration.KAFKA);
    private static final Proxy DATABASE_LINK;

    static {
        POSTGRES.start();
        TOXIPROXY_CONTAINER.start();
        KAFKA.start();
        try {
            DATABASE_LINK = new ToxiproxyClient(TOXIPROXY_CONTAINER.getHost(), TOXIPROXY_CONTAINER.getControlPort())
                    .createProxy("postgres", "0.0.0.0:" + PROXY_PORT, "postgres:5432");
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @DynamicPropertySource
    static void connections(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", () -> "jdbc:postgresql://%s:%d/%s".formatted(
                TOXIPROXY_CONTAINER.getHost(), TOXIPROXY_CONTAINER.getMappedPort(PROXY_PORT),
                POSTGRES.getDatabaseName()));
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
        registry.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
    }

    @Autowired
    private TransferService service;
    @Autowired
    private AccountRepository accounts;
    @Autowired
    private OutboxRelay relay;
    @Autowired
    private StatementProjection projection;
    @Autowired
    private Reconciliation reconciliation;
    @Autowired
    private JdbcClient jdbc;
    @Autowired
    private MeterRegistry meters;

    private final Random random = new Random(20261116L);

    @AfterEach
    void restore() throws IOException {
        relay.setAfterSendHook(null);
        projection.setAfterApplyHook(null);
        DATABASE_LINK.enable();
    }

    @Test
    void brokerDown() {
        List<UUID> ids = openAccounts();
        pauseKafka();
        try {
            transfers(ids, TRANSFERS);
            // The relay's sends time out: the failure is recorded and every row stays pending.
            await().atMost(RECOVERY).until(() -> count("""
                    select count(*) from outbox_events
                    where published_at is null and attempts > 0 and last_error like '%Timeout%'
                    """) >= TRANSFERS);
        } finally {
            unpauseKafka();
        }
        assertRecovered();
    }

    @Test
    void databaseConnectionCutBetweenSendAndMarkPublished() throws Exception {
        List<UUID> ids = openAccounts();
        assertRecovered(); // the deposits are out, so the batch that is cut holds only the transfers
        double duplicatesBefore = duplicates();
        CountDownLatch cut = new CountDownLatch(1);
        // Keep the relay off while the transfers commit, so they all go out in the batch that is cut.
        // This connection bypasses the proxy, like a second relay instance would.
        try (Connection direct = DriverManager.getConnection(
                POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword());
                Statement sql = direct.createStatement()) {
            sql.execute("select pg_advisory_lock(4242, 1)");
            transfers(ids, TRANSFERS);
            AtomicBoolean armed = new AtomicBoolean(true);
            relay.setAfterSendHook(() -> {
                // Kafka acknowledged the whole batch; the mark-published update has not run yet.
                if (armed.getAndSet(false)) {
                    disableDatabaseLink();
                    cut.countDown();
                }
            });
            sql.execute("select pg_advisory_unlock(4242, 1)");
        }
        assertThat(cut.await(30, TimeUnit.SECONDS)).as("relay reached the cut").isTrue();
        Thread.sleep(2_000); // the relay's update fails, its transaction is gone with the connection
        DATABASE_LINK.enable();

        // Every event of the cut batch is published twice and applied once.
        awaitDuplicates(duplicatesBefore, TRANSFERS);
        assertRecovered();
    }

    @Test
    void consumerCrashesAfterItsCommitBeforeTheOffsetCommit() {
        List<UUID> ids = openAccounts();
        assertRecovered();
        double duplicatesBefore = duplicates();
        Set<UUID> crashed = ConcurrentHashMap.newKeySet();
        projection.setAfterApplyHook(event -> {
            if (crashed.add(event.eventId())) {
                throw new IllegalStateException("simulated crash after the statement commit");
            }
        });

        transfers(ids, TRANSFERS);

        // Every event crashed once after its commit, came back and was recognized as applied.
        awaitDuplicates(duplicatesBefore, TRANSFERS);
        assertThat(crashed).hasSizeGreaterThanOrEqualTo(TRANSFERS);
        assertRecovered();
    }

    @Test
    void duplicateDelivery() {
        List<UUID> ids = openAccounts();
        transfers(ids, TRANSFERS);
        assertRecovered();
        double duplicatesBefore = duplicates();

        // Forced re-send of everything already published.
        int resent = jdbc.sql("update outbox_events set published_at = null where published_at is not null").update();

        awaitDuplicates(duplicatesBefore, resent);
        assertRecovered();
    }

    /** Phase 3 invariants 2 to 5 over everything the test class has written so far. */
    private void assertRecovered() {
        await().atMost(RECOVERY).ignoreExceptions().untilAsserted(() -> {
            assertThat(count("select count(*) from outbox_events where published_at is null")).isZero();
            assertThat(count("select count(*) from statement.statement_lines"))
                    .isEqualTo(2 * count("select count(*) from transfers"));
        });
        // Transient failures were retried, never dead-lettered.
        assertThat(meters.counter("ledger.statement.dead-lettered").count()).isZero();
        assertThat(count("select count(*) from statement.processed_events"))
                .isEqualTo(count("select count(*) from outbox_events"));
        ReconciliationReport report = reconciliation.reconcileAll(Instant.now());
        assertThat(report.isClean()).as("reconciliation %s", report).isTrue();
        assertThat(count("""
                select count(*) from accounts a
                where a.balance_cents <> coalesce(
                    (select sum(l.amount_cents) from statement.statement_lines l where l.account_id = a.id), 0)
                """)).isZero();
        assertThat(count("""
                select count(*) from (
                    select a.kind,
                           sum(l.amount_cents) over (partition by l.account_id order by l.sequence) as running,
                           l.occurred_at < lag(l.occurred_at) over (partition by l.account_id order by l.sequence)
                               as went_back
                    from statement.statement_lines l join accounts a on a.id = l.account_id
                ) ordered
                where (ordered.kind = 'CUSTOMER' and ordered.running < 0) or ordered.went_back
                """)).isZero();
    }

    private List<UUID> openAccounts() {
        List<UUID> ids = new ArrayList<>();
        for (int i = 0; i < 4; i++) {
            UUID id = accounts.save(Account.openCustomer()).getId();
            service.deposit(UUID.randomUUID().toString(), id, new Money(100_00));
            ids.add(id);
        }
        return ids;
    }

    /** Small amounts, so every transfer succeeds and {@code n} events are written. */
    private void transfers(List<UUID> ids, int n) {
        for (int i = 0; i < n; i++) {
            int from = random.nextInt(ids.size());
            int to = (from + 1 + random.nextInt(ids.size() - 1)) % ids.size();
            service.transfer(UUID.randomUUID().toString(), ids.get(from), ids.get(to), new Money(1 + random.nextInt(100)));
        }
    }

    private static void pauseKafka() {
        DockerClientFactory.instance().client().pauseContainerCmd(KAFKA.getContainerId()).exec();
    }

    private static void unpauseKafka() {
        DockerClientFactory.instance().client().unpauseContainerCmd(KAFKA.getContainerId()).exec();
    }

    private static void disableDatabaseLink() {
        try {
            DATABASE_LINK.disable();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private void awaitDuplicates(double before, int atLeast) {
        await().atMost(RECOVERY).untilAsserted(() -> assertThat(duplicates() - before)
                .as("duplicate deliveries recognized").isGreaterThanOrEqualTo(atLeast));
    }

    private double duplicates() {
        return meters.counter("ledger.statement.duplicates").count();
    }

    private long count(String sql) {
        return jdbc.sql(sql).query(Long.class).single();
    }
}
