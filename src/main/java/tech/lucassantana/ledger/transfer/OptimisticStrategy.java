package tech.lucassantana.ledger.transfer;

import java.util.UUID;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;
import tech.lucassantana.ledger.account.Account;
import tech.lucassantana.ledger.account.AccountNotFoundException;
import tech.lucassantana.ledger.account.AccountRepository;

/**
 * Reads both accounts without any lock and writes at commit with {@code where version = ?}
 * (ADR 0010). If another transfer changed either account in between, the update matches no row,
 * Hibernate raises an optimistic locking failure, and {@link TransferService} retries the whole
 * transfer on fresh data.
 *
 * <p>The balance check runs on the read values; it stays sound because a stale read can never
 * be written back. Updates are flushed in id order ({@code hibernate.order_updates}), so two
 * transfers over the same pair of accounts conflict instead of deadlocking.
 */
@Component
@ConditionalOnProperty(name = "ledger.concurrency.strategy", havingValue = "optimistic")
class OptimisticStrategy implements ConcurrencyStrategy {

    private final AccountRepository accounts;

    OptimisticStrategy(AccountRepository accounts) {
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

    private Account read(UUID id) {
        return accounts.findById(id).orElseThrow(() -> new AccountNotFoundException(id));
    }
}
