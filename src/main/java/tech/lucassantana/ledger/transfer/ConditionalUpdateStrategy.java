package tech.lucassantana.ledger.transfer;

import java.util.UUID;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;
import tech.lucassantana.ledger.account.Account;
import tech.lucassantana.ledger.account.AccountNotFoundException;
import tech.lucassantana.ledger.account.AccountRepository;
import tech.lucassantana.ledger.account.InsufficientFundsException;

/**
 * One conditional {@code UPDATE} per account and no read of the balance before writing
 * (ADR 0010). The debit only applies when the row still covers it, checked by the statement that
 * writes under its own row lock, so two concurrent debits cannot both pass on a stale read.
 *
 * <p>The two statements run in id order, like the pessimistic strategy, so A->B and B->A wait
 * for each other instead of deadlocking. Only when a statement matches no row does the strategy
 * read the account, to tell the client why.
 */
@Component
@ConditionalOnProperty(name = "ledger.concurrency.strategy", havingValue = "conditional-update")
class ConditionalUpdateStrategy implements ConcurrencyStrategy {

    private final AccountRepository accounts;

    ConditionalUpdateStrategy(AccountRepository accounts) {
        this.accounts = accounts;
    }

    @Override
    public void move(UUID fromId, UUID toId, long cents, boolean sourceMayBeSettlement) {
        if (fromId.compareTo(toId) < 0) {
            debit(fromId, cents, sourceMayBeSettlement);
            credit(toId, cents);
        } else {
            credit(toId, cents);
            debit(fromId, cents, sourceMayBeSettlement);
        }
    }

    private void debit(UUID id, long cents, boolean settlementAllowed) {
        if (accounts.debitIfCovered(id, cents, settlementAllowed) == 0) {
            Account account = existing(id);
            throw account.isCustomer() ? new InsufficientFundsException(id) : settlementUsedDirectly();
        }
    }

    private void credit(UUID id, long cents) {
        if (accounts.creditCustomer(id, cents) == 0) {
            existing(id);
            throw settlementUsedDirectly();
        }
    }

    private Account existing(UUID id) {
        return accounts.findById(id).orElseThrow(() -> new AccountNotFoundException(id));
    }

    private static InvalidTransferException settlementUsedDirectly() {
        return new InvalidTransferException("the settlement account cannot be used directly");
    }
}
