package tech.lucassantana.ledger.transfer;

import java.util.UUID;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;
import tech.lucassantana.ledger.account.Account;
import tech.lucassantana.ledger.account.AccountNotFoundException;
import tech.lucassantana.ledger.account.AccountRepository;

/** {@code SELECT ... FOR UPDATE} on both accounts, smaller id first (ADR 0007). The default. */
@Component
@ConditionalOnProperty(name = "ledger.concurrency.strategy", havingValue = "pessimistic", matchIfMissing = true)
class PessimisticStrategy implements ConcurrencyStrategy {

    private final AccountRepository accounts;

    PessimisticStrategy(AccountRepository accounts) {
        this.accounts = accounts;
    }

    @Override
    public void move(UUID fromId, UUID toId, long cents, boolean sourceMayBeSettlement) {
        // One global lock order, so that A->B and B->A running at the same time wait for each
        // other instead of deadlocking.
        boolean fromFirst = fromId.compareTo(toId) < 0;
        Account first = lock(fromFirst ? fromId : toId);
        Account second = lock(fromFirst ? toId : fromId);
        Account source = fromFirst ? first : second;
        Account destination = fromFirst ? second : first;
        if (!destination.isCustomer() || (!sourceMayBeSettlement && !source.isCustomer())) {
            throw new InvalidTransferException("the settlement account cannot be used directly");
        }
        source.debit(cents);
        destination.credit(cents);
    }

    private Account lock(UUID id) {
        return accounts.findByIdForUpdate(id).orElseThrow(() -> new AccountNotFoundException(id));
    }
}
