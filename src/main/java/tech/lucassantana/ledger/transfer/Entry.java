package tech.lucassantana.ledger.transfer;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import java.time.Instant;
import java.util.UUID;
import org.hibernate.annotations.Immutable;

/**
 * One side of a transfer: negative on the debited account, positive on the credited one.
 * Append-only; the database rejects updates and deletes.
 */
@Entity
@Immutable
@Table(name = "entries")
public class Entry {

    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    private UUID id;

    @Column(name = "transfer_id", nullable = false, updatable = false)
    private UUID transferId;

    @Column(name = "account_id", nullable = false, updatable = false)
    private UUID accountId;

    @Column(name = "amount_cents", nullable = false, updatable = false)
    private long amountCents;

    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt;

    protected Entry() {
        // for JPA
    }

    private Entry(Transfer transfer, UUID accountId, long amountCents) {
        this.transferId = transfer.getId();
        this.accountId = accountId;
        this.amountCents = amountCents;
        this.createdAt = transfer.getCreatedAt();
    }

    static Entry debit(Transfer transfer) {
        return new Entry(transfer, transfer.getFromAccountId(), -transfer.getAmountCents());
    }

    static Entry credit(Transfer transfer) {
        return new Entry(transfer, transfer.getToAccountId(), transfer.getAmountCents());
    }
}
