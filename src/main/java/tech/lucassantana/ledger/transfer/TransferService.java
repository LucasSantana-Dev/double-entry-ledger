package tech.lucassantana.ledger.transfer;

import java.util.List;
import java.util.Optional;
import java.util.UUID;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import tech.lucassantana.ledger.account.Account;
import tech.lucassantana.ledger.account.AccountNotFoundException;
import tech.lucassantana.ledger.account.AccountRepository;
import tech.lucassantana.ledger.idempotency.IdempotencyKey;
import tech.lucassantana.ledger.idempotency.IdempotencyKeyInProgressException;
import tech.lucassantana.ledger.idempotency.IdempotencyKeyRepository;
import tech.lucassantana.ledger.idempotency.IdempotencyKeyReusedException;
import tech.lucassantana.ledger.money.Money;

@Service
public class TransferService {

    private enum Operation { TRANSFER, DEPOSIT }

    private final AccountRepository accounts;
    private final TransferRepository transfers;
    private final EntryRepository entries;
    private final IdempotencyKeyRepository keys;

    TransferService(
            AccountRepository accounts,
            TransferRepository transfers,
            EntryRepository entries,
            IdempotencyKeyRepository keys) {
        this.accounts = accounts;
        this.transfers = transfers;
        this.entries = entries;
        this.keys = keys;
    }

    /** Moves money between two customer accounts. */
    @Transactional
    public TransferOutcome transfer(String idempotencyKey, UUID fromAccountId, UUID toAccountId, Money amount) {
        return move(idempotencyKey, Operation.TRANSFER, fromAccountId, toAccountId, amount);
    }

    /** Brings money into a customer account; the settlement account is the other side (ADR 0008). */
    @Transactional
    public TransferOutcome deposit(String idempotencyKey, UUID accountId, Money amount) {
        return move(idempotencyKey, Operation.DEPOSIT, Account.SETTLEMENT_ID, accountId, amount);
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

        // 2. Lock both rows in one global order, smaller id first, so that A->B and B->A
        //    running at the same time wait for each other instead of deadlocking (ADR 0007).
        boolean fromFirst = fromId.compareTo(toId) < 0;
        Account first = lock(fromFirst ? fromId : toId);
        Account second = lock(fromFirst ? toId : fromId);
        Account source = fromFirst ? first : second;
        Account destination = fromFirst ? second : first;
        if (!destination.isCustomer() || (operation == Operation.TRANSFER && !source.isCustomer())) {
            throw new InvalidTransferException("the settlement account cannot be used directly");
        }

        // 3. Move the money and record why it moved, all in this transaction. Any failure above
        //    or below rolls back everything, the idempotency key included.
        source.debit(amount.cents());
        destination.credit(amount.cents());
        Transfer transfer = transfers.save(new Transfer(fromId, toId, amount.cents()));
        entries.saveAll(List.of(Entry.debit(transfer), Entry.credit(transfer)));
        keys.save(new IdempotencyKey(key, fingerprint, transfer.getId()));
        return new TransferOutcome(transfer, false);
    }

    private TransferOutcome replay(String key, IdempotencyKey existing, String fingerprint) {
        if (!existing.matches(fingerprint)) {
            throw new IdempotencyKeyReusedException(key);
        }
        Transfer original = transfers.findById(existing.getTransferId()).orElseThrow();
        return new TransferOutcome(original, true);
    }

    private Account lock(UUID id) {
        return accounts.findByIdForUpdate(id).orElseThrow(() -> new AccountNotFoundException(id));
    }
}
