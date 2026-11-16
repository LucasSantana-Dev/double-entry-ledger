package tech.lucassantana.ledger.outbox;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.sql.Connection;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.sql.DataSource;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.testcontainers.kafka.KafkaContainer;
import tech.lucassantana.ledger.TestcontainersConfiguration;
import tech.lucassantana.ledger.TopicReader;
import tech.lucassantana.ledger.account.Account;
import tech.lucassantana.ledger.account.AccountRepository;
import tech.lucassantana.ledger.money.Money;
import tech.lucassantana.ledger.transfer.TransferService;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;

/** The relay of ADR 0016 against a real PostgreSQL and a real Kafka broker. */
@SpringBootTest
@Import(TestcontainersConfiguration.class)
class OutboxRelayTest {

    private static final Duration WAIT = Duration.ofSeconds(30);

    @Autowired
    private TransferService service;
    @Autowired
    private AccountRepository accounts;
    @Autowired
    private OutboxRelay relay;
    @Autowired
    private JdbcClient jdbc;
    @Autowired
    private DataSource dataSource;
    @Autowired
    private KafkaContainer kafka;
    @Autowired
    private JsonMapper json;
    @Value("${ledger.outbox.topic}")
    private String topic;

    @Test
    void publishesEveryCommittedTransferOnceKeyedByTransferIdInTheEnvelope() {
        UUID a = openWith(100_00);
        UUID b = openWith(0);
        List<UUID> transfers = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            transfers.add(service.transfer(key(), a, b, new Money(10_00)).transfer().getId());
        }
        transfers.add(service.transfer(key(), b, a, new Money(5_00)).transfer().getId());

        awaitPublished(transfers);
        Map<UUID, List<ConsumerRecord<String, String>>> byTransfer = recordsFor(Set.copyOf(transfers));

        for (UUID transferId : transfers) {
            assertThat(byTransfer.get(transferId)).as("records for %s", transferId).hasSize(1);
            ConsumerRecord<String, String> record = byTransfer.get(transferId).getFirst();
            assertThat(record.key()).isEqualTo(transferId.toString());
            JsonNode event = json.readTree(record.value());
            var row = jdbc.sql("select id, event_id from outbox_events where aggregate_id = ?")
                    .param(transferId)
                    .query((rs, n) -> Map.entry(rs.getLong("id"), rs.getObject("event_id", UUID.class)))
                    .single();
            assertThat(event.get("eventId").asString()).isEqualTo(row.getValue().toString());
            assertThat(event.get("sequence").asLong()).isEqualTo(row.getKey());
            assertThat(event.get("eventType").asString()).isEqualTo("TransferCommitted");
            assertThat(event.get("schemaVersion").asInt()).isEqualTo(1);
            assertThat(event.get("aggregateType").asString()).isEqualTo("transfer");
            assertThat(event.get("aggregateId").asString()).isEqualTo(transferId.toString());
            assertThat(event.get("occurredAt").isString()).isTrue();
            assertThat(event.get("payload").get("transferId").asString()).isEqualTo(transferId.toString());
            assertThat(event.get("payload").get("amountCents").isIntegralNumber()).isTrue();
        }
        JsonNode payload = json.readTree(byTransfer.get(transfers.getLast()).getFirst().value()).get("payload");
        assertThat(payload.get("fromAccountId").asString()).isEqualTo(b.toString());
        assertThat(payload.get("toAccountId").asString()).isEqualTo(a.toString());
        assertThat(payload.get("amountCents").asLong()).isEqualTo(5_00);
    }

    @Test
    void sequenceFollowsCommitOrderOfAnAccount() {
        UUID a = openWith(100_00);
        UUID b = openWith(0);
        List<UUID> inCommitOrder = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            // Each call returns after its commit, so this list is the commit order on a and b.
            boolean forward = i % 3 != 2;
            inCommitOrder.add(service.transfer(key(), forward ? a : b, forward ? b : a, new Money(1_00))
                    .transfer().getId());
        }

        List<UUID> bySequence = jdbc.sql("""
                        select aggregate_id from outbox_events where aggregate_id in (:ids) order by id
                        """)
                .param("ids", inCommitOrder)
                .query(UUID.class)
                .list();
        assertThat(bySequence).containsExactlyElementsOf(inCommitOrder);
    }

    @Test
    void onlyOneRelayWorksAtATime() throws Exception {
        UUID a = openWith(10_00);
        try (Connection other = dataSource.getConnection(); Statement sql = other.createStatement()) {
            // Another relay instance holds the lock (session-level here, transaction-level in the relay).
            sql.execute("select pg_advisory_lock(4242, 1)");
            UUID transferId = service.transfer(key(), a, openWith(0), new Money(1_00)).transfer().getId();

            assertThat(relay.relayBatch()).isZero();
            assertThat(publishedAt(transferId)).isNull();

            sql.execute("select pg_advisory_unlock(4242, 1)");
            awaitPublished(List.of(transferId));
        }
    }

    @Test
    void parksAPoisonMessageAndKeepsPublishingTheRest() {
        // Bigger than the producer's 1 MB max.request.size: Kafka will never accept it.
        UUID poisonId = UUID.randomUUID();
        jdbc.sql("""
                        insert into outbox_events
                            (event_id, aggregate_type, aggregate_id, event_type, schema_version, payload, occurred_at)
                        values (:eventId, 'transfer', :aggregateId, 'TransferCommitted', 1,
                                jsonb_build_object('padding', repeat('x', 2 * 1024 * 1024)), now())
                        """)
                .param("eventId", UUID.randomUUID())
                .param("aggregateId", poisonId)
                .update();
        UUID transferId = service.transfer(key(), openWith(10_00), openWith(0), new Money(1_00)).transfer().getId();

        awaitPublished(List.of(transferId));
        await().atMost(WAIT).untilAsserted(() -> {
            var poison = jdbc.sql("select parked_at, published_at, last_error from outbox_events where aggregate_id = ?")
                    .param(poisonId)
                    .query((rs, n) -> new Object[] {rs.getObject("parked_at"), rs.getObject("published_at"),
                            rs.getString("last_error")})
                    .single();
            assertThat(poison[0]).as("parked_at").isNotNull();
            assertThat(poison[1]).as("published_at").isNull();
            assertThat((String) poison[2]).contains("RecordTooLargeException");
        });
    }

    @Test
    void cleanupDeletesOnlyPublishedRowsOlderThanTheCutoff() {
        UUID transferId = service.transfer(key(), openWith(10_00), openWith(0), new Money(1_00)).transfer().getId();
        awaitPublished(List.of(transferId));
        UUID parkedId = UUID.randomUUID();
        jdbc.sql("""
                        insert into outbox_events
                            (event_id, aggregate_type, aggregate_id, event_type, schema_version, payload, occurred_at,
                             parked_at)
                        values (:eventId, 'transfer', :aggregateId, 'TransferCommitted', 1, '{}', now(), now())
                        """)
                .param("eventId", UUID.randomUUID())
                .param("aggregateId", parkedId)
                .update();

        assertThat(relay.deletePublishedBefore(Instant.now().minus(Duration.ofDays(1)))).isZero();
        assertThat(rowsFor(transferId)).isOne();

        assertThat(relay.deletePublishedBefore(Instant.now().plusSeconds(1))).isPositive();
        assertThat(rowsFor(transferId)).isZero();
        assertThat(rowsFor(parkedId)).isOne();
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

    private Object publishedAt(UUID transferId) {
        return jdbc.sql("select published_at from outbox_events where aggregate_id = ?")
                .param(transferId)
                .query((rs, n) -> rs.getObject(1))
                .optional()
                .orElse(null);
    }

    private long rowsFor(UUID aggregateId) {
        return jdbc.sql("select count(*) from outbox_events where aggregate_id = ?")
                .param(aggregateId)
                .query(Long.class)
                .single();
    }

    private void awaitPublished(List<UUID> transferIds) {
        await().atMost(WAIT).untilAsserted(() -> assertThat(jdbc.sql("""
                        select count(*) from outbox_events where aggregate_id in (:ids) and published_at is null
                        """)
                .param("ids", transferIds)
                .query(Long.class)
                .single()).isZero());
    }

    private Map<UUID, List<ConsumerRecord<String, String>>> recordsFor(Set<UUID> transferIds) {
        Function<ConsumerRecord<String, String>, UUID> transferOf = r -> UUID.fromString(r.key());
        // Every row is marked published only after Kafka acknowledged it, so all records are there.
        List<ConsumerRecord<String, String>> records = TopicReader.readUntil(kafka.getBootstrapServers(), topic,
                read -> read.stream().map(transferOf).collect(Collectors.toSet()).containsAll(transferIds), WAIT);
        return records.stream()
                .filter(r -> transferIds.contains(transferOf.apply(r)))
                .collect(Collectors.groupingBy(transferOf));
    }
}
