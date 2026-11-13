package tech.lucassantana.ledger.transfer;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.TestPropertySource;
import org.springframework.jdbc.core.simple.JdbcClient;
import tech.lucassantana.ledger.TestcontainersConfiguration;
import tech.lucassantana.ledger.account.Account;
import tech.lucassantana.ledger.account.AccountRepository;
import tech.lucassantana.ledger.account.InsufficientFundsException;
import tech.lucassantana.ledger.idempotency.IdempotencyKeyInProgressException;
import tech.lucassantana.ledger.money.Money;

/**
 * The phase 1 acceptance test: throws thousands of concurrent transfers at a real PostgreSQL,
 * with a hot account, transfers in both directions and duplicated submissions racing the
 * original, then checks every invariant of the spec in the database and against a client-side
 * model of what should have happened.
 *
 * <p>Runs once per concurrency strategy (ADR 0010), each in its own application context and
 * database, with the same assertions: the invariants are the contract every strategy must meet.
 *
 * <p>Seeded and sized by system properties, so any failure reproduces:
 * {@code ./mvnw test -Dtest=ConcurrentTransfersInvariantTest -Dledger.invariant.operations=20000 -Dledger.invariant.seed=7}
 */
@SpringBootTest
@Import(TestcontainersConfiguration.class)
class ConcurrentTransfersInvariantTest {

    private static final int ACCOUNTS = 8;
    private static final long INITIAL_CENTS = 100_000; // 1000.00 each
    private static final long MAX_TRANSFER_CENTS = 50_000; // up to 500.00, so some transfers must fail
    private static final double DUPLICATE_RATE = 0.10;
    private static final double HOT_ACCOUNT_RATE = 0.30;
    /** Requests in flight at once: enough to keep every pooled connection contended, bounded so none times out. */
    private static final int IN_FLIGHT = 64;
    private static final int OPERATIONS = Integer.getInteger("ledger.invariant.operations", 5_000);
    private static final long SEED = Long.getLong("ledger.invariant.seed", 20261113L);

    private enum Result { CREATED, REPLAYED, INSUFFICIENT_FUNDS }

    private record Operation(String key, UUID from, UUID to, long cents) {
    }

    /** {@code transferId} is null for {@link Result#INSUFFICIENT_FUNDS}. */
    private record Outcome(Result result, UUID transferId) {
    }

    private record StoredTransfer(UUID id, UUID from, UUID to, long cents) {
    }

    /** One per strategy. {@code @TestPropertySource} gives each its own context, so its own database. */
    @Nested
    @TestPropertySource(properties = "ledger.concurrency.strategy=pessimistic")
    class Pessimistic {

        @Test
        void ledgerStaysConsistent(@Autowired TransferService service, @Autowired AccountRepository accounts,
                @Autowired JdbcClient jdbc, @Autowired ConcurrencyStrategy strategy) throws InterruptedException {
            assertThat(strategy).isInstanceOf(PessimisticStrategy.class);
            verify(service, accounts, jdbc, "pessimistic");
        }
    }

    // Set by verify() from the nested class's context, never from the enclosing class's.
    private TransferService service;
    private AccountRepository accounts;
    private JdbcClient jdbc;

    private void verify(TransferService service, AccountRepository accounts, JdbcClient jdbc, String strategy)
            throws InterruptedException {
        this.service = service;
        this.accounts = accounts;
        this.jdbc = jdbc;
        List<UUID> ids = IntStream.range(0, ACCOUNTS)
                .mapToObj(i -> accounts.save(Account.openCustomer()).getId())
                .toList();
        ids.forEach(id -> service.deposit(UUID.randomUUID().toString(), id, new Money(INITIAL_CENTS)));

        List<Operation> submissions = workload(ids, new Random(SEED));
        Map<String, List<Outcome>> outcomesByKey = new ConcurrentHashMap<>();
        List<Throwable> unexpected = Collections.synchronizedList(new ArrayList<>());
        AtomicInteger conflicts = new AtomicInteger();
        Semaphore inFlight = new Semaphore(IN_FLIGHT);

        long start = System.nanoTime();
        try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
            for (Operation operation : submissions) {
                inFlight.acquire();
                executor.submit(() -> {
                    try {
                        Outcome outcome = submitLikeAClient(operation, conflicts);
                        outcomesByKey.computeIfAbsent(operation.key(), k -> Collections.synchronizedList(new ArrayList<>()))
                                .add(outcome);
                    } catch (Throwable t) {
                        unexpected.add(t);
                    } finally {
                        inFlight.release();
                    }
                });
            }
        } // close() waits for every submission
        Duration elapsed = Duration.ofNanos(System.nanoTime() - start);

        // No deadlock, lock timeout, constraint violation or any other surprise.
        assertThat(unexpected).isEmpty();

        // The workload really exercised every path, so the assertions below are not vacuous.
        long created = count(outcomesByKey, Result.CREATED);
        long replayed = count(outcomesByKey, Result.REPLAYED);
        long insufficient = count(outcomesByKey, Result.INSUFFICIENT_FUNDS);
        assertThat(created).as("transfers created").isPositive();
        assertThat(replayed).as("duplicates answered by replay").isPositive();
        assertThat(insufficient).as("transfers refused for insufficient funds").isPositive();
        assertThat(conflicts.get()).as("duplicates answered 409 while the original was running").isPositive();

        // Invariant 5: a key never produced two transfers, and every replay returned the original one.
        assertThat(outcomesByKey.values()).allSatisfy(outcomes -> {
            assertThat(outcomes.stream().filter(o -> o.result() == Result.CREATED).count()).isLessThanOrEqualTo(1);
            assertThat(outcomes.stream().map(Outcome::transferId).filter(id -> id != null).distinct().count())
                    .isLessThanOrEqualTo(1);
        });

        // Invariant 1: money is neither created nor destroyed.
        assertThat(count("select coalesce(sum(balance_cents), 0) from accounts")).isZero();

        // Invariant 2: no customer went negative.
        assertThat(count("select count(*) from accounts where kind = 'CUSTOMER' and balance_cents < 0")).isZero();

        // Invariant 3: every balance is exactly the sum of its entries.
        assertThat(count("""
                select count(*) from accounts a
                where a.balance_cents <> coalesce((select sum(e.amount_cents) from entries e where e.account_id = a.id), 0)
                """)).isZero();

        // Invariant 4: every transfer is two entries that cancel out.
        assertThat(count("""
                select count(*) from (
                    select transfer_id from entries group by transfer_id
                    having count(*) <> 2 or sum(amount_cents) <> 0
                ) unbalanced
                """)).isZero();

        // Invariant 6: every transfer has its key stored.
        assertThat(count("""
                select count(*) from transfers t
                where not exists (select 1 from idempotency_keys k where k.transfer_id = t.id)
                """)).isZero();

        // Client-side model: the database holds exactly the transfers reported as created, with the
        // requested accounts and amounts, and each final balance is what those transfers imply.
        assertMatchesClientModel(ids, submissions, outcomesByKey, created);

        System.out.printf(
                "invariant run (%s, seed %d): %d submissions over %d keys in %d ms -> %d created, %d replayed, "
                        + "%d insufficient funds, %d x 409 retried, 0 invariant violations%n",
                strategy, SEED, submissions.size(), outcomesByKey.size(), elapsed.toMillis(),
                created, replayed, insufficient, conflicts.get());
    }

    private void assertMatchesClientModel(
            List<UUID> ids, List<Operation> submissions, Map<String, List<Outcome>> outcomesByKey, long created) {
        Map<UUID, Operation> expectedByTransferId = new HashMap<>();
        Map<UUID, Long> expectedBalance = new HashMap<>();
        ids.forEach(id -> expectedBalance.put(id, INITIAL_CENTS));
        for (Operation operation : new LinkedHashSet<>(submissions)) { // duplicates collapse to one operation
            outcomesByKey.get(operation.key()).stream()
                    .filter(o -> o.result() == Result.CREATED)
                    .findFirst()
                    .ifPresent(o -> {
                        expectedByTransferId.put(o.transferId(), operation);
                        expectedBalance.merge(operation.from(), -operation.cents(), Long::sum);
                        expectedBalance.merge(operation.to(), operation.cents(), Long::sum);
                    });
        }

        List<StoredTransfer> stored = jdbc.sql("""
                        select id, from_account_id, to_account_id, amount_cents from transfers
                        where from_account_id in (:ids) and to_account_id in (:ids)
                        """)
                .param("ids", ids)
                .query((rs, row) -> new StoredTransfer(
                        rs.getObject("id", UUID.class),
                        rs.getObject("from_account_id", UUID.class),
                        rs.getObject("to_account_id", UUID.class),
                        rs.getLong("amount_cents")))
                .list();
        assertThat(stored).hasSize((int) created);
        assertThat(stored).allSatisfy(transfer -> {
            Operation expected = expectedByTransferId.get(transfer.id());
            assertThat(expected).as("transfer %s was reported as created", transfer.id()).isNotNull();
            assertThat(transfer.from()).isEqualTo(expected.from());
            assertThat(transfer.to()).isEqualTo(expected.to());
            assertThat(transfer.cents()).isEqualTo(expected.cents());
        });

        ids.forEach(id -> assertThat(jdbc.sql("select balance_cents from accounts where id = ?")
                        .param(id).query(Long.class).single())
                .as("balance of %s", id)
                .isEqualTo(expectedBalance.get(id)));
    }

    /** Random transfers between the accounts; a share hits one hot account, a share is sent twice in a row. */
    private static List<Operation> workload(List<UUID> ids, Random random) {
        List<Operation> submissions = new ArrayList<>();
        for (int i = 0; i < OPERATIONS; i++) {
            UUID from = pick(ids, random);
            UUID to = pick(ids, random);
            while (to.equals(from)) {
                to = ids.get(random.nextInt(ids.size()));
            }
            Operation operation = new Operation(
                    UUID.randomUUID().toString(), from, to, random.nextLong(1, MAX_TRANSFER_CENTS + 1));
            submissions.add(operation);
            if (random.nextDouble() < DUPLICATE_RATE) {
                submissions.add(operation); // a client retry racing its own original
            }
        }
        return submissions;
    }

    private static UUID pick(List<UUID> ids, Random random) {
        return random.nextDouble() < HOT_ACCOUNT_RATE ? ids.getFirst() : ids.get(random.nextInt(ids.size()));
    }

    /** A well-behaved client: 409 means "still running", so it waits and retries with the same key. */
    private Outcome submitLikeAClient(Operation operation, AtomicInteger conflicts) throws InterruptedException {
        for (int attempt = 0; attempt < 1_000; attempt++) {
            try {
                TransferOutcome outcome = service.transfer(
                        operation.key(), operation.from(), operation.to(), new Money(operation.cents()));
                return new Outcome(outcome.replayed() ? Result.REPLAYED : Result.CREATED, outcome.transfer().getId());
            } catch (IdempotencyKeyInProgressException e) {
                conflicts.incrementAndGet();
                Thread.sleep(5);
            } catch (InsufficientFundsException e) {
                return new Outcome(Result.INSUFFICIENT_FUNDS, null);
            }
        }
        throw new IllegalStateException("key " + operation.key() + " stayed in progress for 1000 attempts");
    }

    private long count(String sql) {
        return jdbc.sql(sql).query(Long.class).single();
    }

    private static long count(Map<String, List<Outcome>> outcomesByKey, Result result) {
        return outcomesByKey.values().stream().flatMap(List::stream).filter(o -> o.result() == result).count();
    }
}
