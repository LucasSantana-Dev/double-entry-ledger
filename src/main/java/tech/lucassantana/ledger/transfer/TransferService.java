package tech.lucassantana.ledger.transfer;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Supplier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.ConcurrencyFailureException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import tech.lucassantana.ledger.account.Account;
import tech.lucassantana.ledger.idempotency.IdempotencyKey;
import tech.lucassantana.ledger.idempotency.IdempotencyKeyInProgressException;
import tech.lucassantana.ledger.idempotency.IdempotencyKeyRepository;
import tech.lucassantana.ledger.idempotency.IdempotencyKeyReusedException;
import tech.lucassantana.ledger.money.Money;
import tech.lucassantana.ledger.outbox.OutboxWriter;

/**
 * Owns its transactions: each public method runs one transaction per attempt and retries the
 * whole attempt when the concurrency strategy aborts on conflict (ADR 0010). Call it outside any
 * transaction, or a retry would run inside a transaction that is already marked rollback-only.
 */
@Service
public class TransferService {

    /** Attempts per request before answering 503 (ADR 0010). */
    static final int MAX_ATTEMPTS = 5;
    /** Backoff ceiling before the second attempt; doubles each time, so about 1, 2, 4, 8 ms. */
    private static final Duration FIRST_BACKOFF = Duration.ofMillis(1);

    private enum Operation { TRANSFER, DEPOSIT }

    private final TransactionTemplate transaction;
    private final Counter retries;
    private final Counter unavailable;
    private final ConcurrencyStrategy concurrency;
    private final TransferRepository transfers;
    private final EntryRepository entries;
    private final IdempotencyKeyRepository keys;
    private final OutboxWriter outbox;

    TransferService(
            PlatformTransactionManager transactionManager,
            MeterRegistry meters,
            @Value("${ledger.concurrency.strategy:conditional-update}") String strategyName,
            ConcurrencyStrategy concurrency,
            TransferRepository transfers,
            EntryRepository entries,
            IdempotencyKeyRepository keys,
            OutboxWriter outbox) {
        this.transaction = new TransactionTemplate(transactionManager);
        this.transaction.setIsolationLevel(concurrency.isolationLevel());
        // Tagged with the configured strategy (ADR 0022), so a dashboard compares strategies.
        this.retries = Counter.builder("ledger.transfer.retries")
                .description("Attempts retried after a concurrency conflict")
                .tag("strategy", strategyName)
                .register(meters);
        this.unavailable = Counter.builder("ledger.transfer.unavailable")
                .description("Requests answered 503 after the retries ran out")
                .tag("strategy", strategyName)
                .register(meters);
        this.concurrency = concurrency;
        this.transfers = transfers;
        this.entries = entries;
        this.keys = keys;
        this.outbox = outbox;
    }

    /** Moves money between two customer accounts. */
    public TransferOutcome transfer(String idempotencyKey, UUID fromAccountId, UUID toAccountId, Money amount) {
        return withRetry(() -> move(idempotencyKey, Operation.TRANSFER, fromAccountId, toAccountId, amount));
    }

    /** Brings money into a customer account; the settlement account is the other side (ADR 0008). */
    public TransferOutcome deposit(String idempotencyKey, UUID accountId, Money amount) {
        return withRetry(() -> move(idempotencyKey, Operation.DEPOSIT, Account.SETTLEMENT_ID, accountId, amount));
    }

    /**
     * Runs the attempt in a fresh transaction, again after a conflict abort, up to
     * {@link #MAX_ATTEMPTS}. A conflict rolled back everything the attempt wrote, the idempotency
     * key and its advisory lock included, so the next attempt starts clean. Any other failure
     * (insufficient funds, unknown account) is a final answer and is not retried.
     */
    private TransferOutcome withRetry(Supplier<TransferOutcome> attempt) {
        for (int n = 1; ; n++) {
            try {
                return transaction.execute(status -> attempt.get());
            } catch (ConcurrencyFailureException conflict) {
                if (n == MAX_ATTEMPTS) {
                    throw unavailable(n, conflict);
                }
                retries.increment();
                try {
                    backoff(n);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw unavailable(n, conflict);
                }
            }
        }
    }

    /** The exception the API answers with 503; counted here so the counter matches the responses. */
    private ConcurrencyConflictException unavailable(int attempts, ConcurrencyFailureException conflict) {
        unavailable.increment();
        return new ConcurrencyConflictException(attempts, conflict);
    }

    /** Exponential backoff with equal jitter: a random pause between half the ceiling and the ceiling. */
    private static void backoff(int attempt) throws InterruptedException {
        long ceiling = FIRST_BACKOFF.toNanos() << (attempt - 1);
        long pause = ceiling / 2 + ThreadLocalRandom.current().nextLong(ceiling / 2 + 1);
        Thread.sleep(Duration.ofNanos(pause));
    }

    private TransferOutcome move(String key, Operation operation, UUID fromId, UUID toId, Money amount) {
        if (!amount.isPositive()) {
            throw new InvalidTransferException("amount must be greater than zero");
        }
        if (fromId.equals(toId)) {
            throw new InvalidTransferException("source and destination must be different accounts");
        }

        // 1. Idempotency (ADR 0006): one request per key at a time, and a committed key replays.
        if (!keys.tryLock(key)) {
            throw new IdempotencyKeyInProgressException(key);
        }
        String fingerprint = operation + ":" + fromId + ":" + toId + ":" + amount.cents();
        Optional<IdempotencyKey> existing = keys.findById(key);
        if (existing.isPresent()) {
            return replay(key, existing.get(), fingerprint);
        }

        // 2. Change both balances under the configured concurrency strategy (ADR 0010).
        concurrency.move(fromId, toId, amount.cents(), operation == Operation.DEPOSIT);

        // 3. Record why the money moved, all in this transaction. Any failure above or below
        //    rolls back everything, the idempotency key and the event included.
        Transfer transfer = transfers.save(new Transfer(fromId, toId, amount.cents()));
        entries.saveAll(List.of(Entry.debit(transfer), Entry.credit(transfer)));
        keys.save(new IdempotencyKey(key, fingerprint, transfer.getId()));

        // 4. The event, last (ADR 0016): its sequence is drawn after step 2 locked or read the
        //    accounts, so per account the sequence follows commit order.
        outbox.transferCommitted(
                transfer.getId(), fromId, toId, amount.cents(), transfer.getCreatedAt());
        return new TransferOutcome(transfer, false);
    }

    private TransferOutcome replay(String key, IdempotencyKey existing, String fingerprint) {
        if (!existing.matches(fingerprint)) {
            throw new IdempotencyKeyReusedException(key);
        }
        Transfer original = transfers.findById(existing.getTransferId()).orElseThrow();
        return new TransferOutcome(original, true);
    }
}
