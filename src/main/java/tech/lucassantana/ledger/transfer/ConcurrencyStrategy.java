package tech.lucassantana.ledger.transfer;

import java.util.UUID;
import org.springframework.transaction.TransactionDefinition;

/**
 * How a transfer changes two balances without lost updates when other transfers touch the same
 * accounts at the same time (ADR 0010). Runs inside the caller's transaction, so a failure rolls
 * back the balances together with the transfer, its entries and its idempotency key.
 *
 * <p>Selected by {@code ledger.concurrency.strategy}.
 */
interface ConcurrencyStrategy {

    /**
     * Debits {@code fromId} and credits {@code toId} by {@code cents}.
     *
     * @param sourceMayBeSettlement true for deposits, where money enters through the settlement account (ADR 0008)
     * @throws tech.lucassantana.ledger.account.AccountNotFoundException if either account does not exist
     * @throws InvalidTransferException if the settlement account is used where it is not allowed
     * @throws tech.lucassantana.ledger.account.InsufficientFundsException if a customer source holds less than {@code cents}
     */
    void move(UUID fromId, UUID toId, long cents, boolean sourceMayBeSettlement);

    /** Isolation level of the transaction {@code move} runs in; the database default (read committed) unless overridden. */
    default int isolationLevel() {
        return TransactionDefinition.ISOLATION_DEFAULT;
    }
}
