package tech.lucassantana.ledger.transfer;

import java.util.UUID;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;
import org.springframework.transaction.TransactionDefinition;
import tech.lucassantana.ledger.account.Account;
import tech.lucassantana.ledger.account.AccountNotFoundException;
import tech.lucassantana.ledger.account.AccountRepository;

/**
 * Plain reads and writes, no explicit lock, inside a {@code SERIALIZABLE} transaction (ADR 0010).
 * PostgreSQL's serializable snapshot isolation guarantees the outcome equals some serial order of
 * the transactions; when it cannot, it aborts one of them with SQLSTATE {@code 40001} and
 * {@link TransferService} retries it, as the PostgreSQL manual requires (section 13.2.3).
 *
 * <p>Reads go in id order and updates are flushed in id order ({@code hibernate.order_updates}),
 * so conflicts surface as serialization failures rather than deadlocks.
 */
@Component
@ConditionalOnProperty(name = "ledger.concurrency.strategy", havingValue = "serializable")
class SerializableStrategy implements ConcurrencyStrategy {

    private final AccountRepository accounts;

    SerializableStrategy(AccountRepository accounts) {
        this.accounts = accounts;
    }

    @Override
    public void move(UUID fromId, UUID toId, long cents, boolean sourceMayBeSettlement) {
        boolean fromFirst = fromId.compareTo(toId) < 0;
        Account first = read(fromFirst ? fromId : toId);
        Account second = read(fromFirst ? toId : fromId);
        Account source = fromFirst ? first : second;
        Account destination = fromFirst ? second : first;
        if (!destination.isCustomer() || (!sourceMayBeSettlement && !source.isCustomer())) {
            throw new InvalidTransferException("the settlement account cannot be used directly");
        }
        source.debit(cents);
        destination.credit(cents);
    }

    @Override
    public int isolationLevel() {
        return TransactionDefinition.ISOLATION_SERIALIZABLE;
    }

    private Account read(UUID id) {
        return accounts.findById(id).orElseThrow(() -> new AccountNotFoundException(id));
    }
}
