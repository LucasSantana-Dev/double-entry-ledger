package tech.lucassantana.ledger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.http.MediaType;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.context.WebApplicationContext;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.kafka.KafkaContainer;
import tech.lucassantana.ledger.account.Account;
import tech.lucassantana.ledger.account.AccountRepository;
import tech.lucassantana.ledger.money.Money;
import tech.lucassantana.ledger.outbox.OutboxMetrics;
import tech.lucassantana.ledger.transfer.TransferService;

/**
 * Phase 4 criteria 4 and 5 (ADR 0022, ADR 0023): the custom meters move as the system does.
 * Criterion 6, the reconciliation gauge, is checked in {@code ReconciliationTest}, the test that
 * produces differences on purpose.
 *
 * <p>The contexts are the ones other tests already start (the default one, and the optimistic and
 * serializable ones of {@code ConcurrentTransfersInvariantTest}), so this class adds no containers.
 * Meters in a shared context carry earlier tests' counts: every check works on differences.
 */
@SpringBootTest
@Import(TestcontainersConfiguration.class)
class ObservabilityMetricsTest {

    private static final Duration RECOVERY = Duration.ofSeconds(120);
    private static final int TRANSFERS = 10;

    @Test
    void outboxGaugesRiseWhileKafkaIsPausedAndPendingReturnsToZeroAfterTheRelayDrains(
            @Autowired TransferService service, @Autowired AccountRepository accounts,
            @Autowired OutboxMetrics outbox, @Autowired MeterRegistry meters, @Autowired KafkaContainer kafka)
            throws InterruptedException {
        UUID from = openWith(service, accounts, 100_00);
        UUID to = openWith(service, accounts, 0);
        awaitDrained(outbox, meters);

        pause(kafka);
        try {
            for (int i = 0; i < TRANSFERS; i++) {
                service.transfer(UUID.randomUUID().toString(), from, to, new Money(1_00));
            }
            // The test refreshes the gauges itself instead of waiting for the schedule.
            outbox.refresh();
            assertThat(gauge(meters, "ledger.outbox.pending")).as("pending").isGreaterThanOrEqualTo(TRANSFERS);
            double age = gauge(meters, "ledger.outbox.oldest.pending.age");
            assertThat(age).as("oldest pending age").isPositive();
            Thread.sleep(1_500);
            outbox.refresh();
            assertThat(gauge(meters, "ledger.outbox.oldest.pending.age")).as("oldest pending age keeps growing")
                    .isGreaterThanOrEqualTo(age + 1.0);
            assertThat(gauge(meters, "ledger.outbox.pending")).isGreaterThanOrEqualTo(TRANSFERS);
        } finally {
            unpause(kafka);
        }

        awaitDrained(outbox, meters);
        assertThat(gauge(meters, "ledger.outbox.oldest.pending.age")).isZero();
    }

    /** Same context as {@code ConcurrentTransfersInvariantTest.Optimistic}. */
    @Nested
    @TestPropertySource(properties = "ledger.concurrency.strategy=optimistic")
    class Optimistic {

        @Test
        void retriesAndUnavailableAreCountedUnderTheStrategyTag(@Autowired WebApplicationContext context,
                @Autowired TransferService service, @Autowired AccountRepository accounts,
                @Autowired MeterRegistry meters) throws Exception {
            hotAccount(context, service, accounts, meters, "optimistic");
        }
    }

    /** Same context as {@code ConcurrentTransfersInvariantTest.Serializable}. */
    @Nested
    @TestPropertySource(properties = "ledger.concurrency.strategy=serializable")
    class Serializable {

        @Test
        void retriesAndUnavailableAreCountedUnderTheStrategyTag(@Autowired WebApplicationContext context,
                @Autowired TransferService service, @Autowired AccountRepository accounts,
                @Autowired MeterRegistry meters) throws Exception {
            hotAccount(context, service, accounts, meters, "serializable");
        }
    }

    /**
     * Criterion 5: every request debits the same account at once, so the strategy aborts and
     * retries; the retries counter grows under this strategy's tag, and the unavailable counter
     * grows by exactly the number of 503 responses.
     */
    private static void hotAccount(WebApplicationContext context, TransferService service,
            AccountRepository accounts, MeterRegistry meters, String strategy) throws Exception {
        MockMvc mvc = MockMvcBuilders.webAppContextSetup(context).build();
        UUID hot = openWith(service, accounts, 1_000_000_00);
        List<UUID> targets = List.of(openWith(service, accounts, 0), openWith(service, accounts, 0),
                openWith(service, accounts, 0), openWith(service, accounts, 0));
        Counter retries = meters.get("ledger.transfer.retries").tag("strategy", strategy).counter();
        Counter unavailable = meters.get("ledger.transfer.unavailable").tag("strategy", strategy).counter();
        double retriesBefore = retries.count();
        double unavailableBefore = unavailable.count();

        List<Future<Integer>> responses = new ArrayList<>();
        try (ExecutorService clients = Executors.newVirtualThreadPerTaskExecutor()) {
            for (int i = 0; i < 400; i++) {
                UUID target = targets.get(i % targets.size());
                responses.add(clients.submit(() -> mvc.perform(post("/transfers")
                                .header("Idempotency-Key", UUID.randomUUID().toString())
                                .contentType(MediaType.APPLICATION_JSON)
                                .content("""
                                        {"fromAccountId": "%s", "toAccountId": "%s", "amount": "0.01"}"""
                                        .formatted(hot, target)))
                        .andReturn().getResponse().getStatus()));
            }
        }
        List<Integer> statuses = new ArrayList<>();
        for (Future<Integer> response : responses) {
            statuses.add(response.get());
        }
        Map<Integer, Long> byStatus = statuses.stream()
                .collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));
        LoggerFactory.getLogger(ObservabilityMetricsTest.class).info("hot account under {}: statuses {}, {} retries",
                strategy, byStatus, retries.count() - retriesBefore);

        assertThat(byStatus.keySet()).as("statuses %s", byStatus).isSubsetOf(201, 503);
        assertThat(retries.count() - retriesBefore).as("retries under strategy=%s, statuses %s", strategy, byStatus)
                .isPositive();
        assertThat(unavailable.count() - unavailableBefore).as("unavailable, statuses %s", byStatus)
                .isEqualTo(byStatus.getOrDefault(503, 0L).doubleValue());
        // One strategy per context, so no other tag value exists here.
        assertThat(meters.find("ledger.transfer.retries").counters())
                .allSatisfy(c -> assertThat(c.getId().getTag("strategy")).isEqualTo(strategy));
    }

    private static UUID openWith(TransferService service, AccountRepository accounts, long cents) {
        UUID id = accounts.save(Account.openCustomer()).getId();
        if (cents > 0) {
            service.deposit(UUID.randomUUID().toString(), id, new Money(cents));
        }
        return id;
    }

    private static void awaitDrained(OutboxMetrics outbox, MeterRegistry meters) {
        await().atMost(RECOVERY).untilAsserted(() -> {
            outbox.refresh();
            assertThat(gauge(meters, "ledger.outbox.pending")).as("pending").isZero();
        });
    }

    private static double gauge(MeterRegistry meters, String name) {
        return meters.get(name).gauge().value();
    }

    private static void pause(KafkaContainer kafka) {
        DockerClientFactory.instance().client().pauseContainerCmd(kafka.getContainerId()).exec();
    }

    private static void unpause(KafkaContainer kafka) {
        DockerClientFactory.instance().client().unpauseContainerCmd(kafka.getContainerId()).exec();
    }
}
