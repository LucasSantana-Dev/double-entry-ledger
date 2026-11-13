package tech.lucassantana.ledger.account;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import jakarta.persistence.Version;
import java.time.Instant;
import java.util.UUID;

/**
 * An account with a materialized balance (ADR 0005). The balance only changes together with
 * the entries that justify it, inside the same transaction, while the row is locked.
 */
@Entity
@Table(name = "accounts")
public class Account {

    /** Created by the V1 migration. */
    public static final UUID SETTLEMENT_ID = UUID.fromString("00000000-0000-0000-0000-000000000001");

    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    private UUID id;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, updatable = false)
    private AccountKind kind;

    @Column(name = "balance_cents", nullable = false)
    private long balanceCents;

    /**
     * Bumped on every balance change. Hibernate adds {@code where version = ?} to each update, so
     * a write based on a stale read fails instead of overwriting (optimistic strategy, ADR 0010).
     * Harmless under the pessimistic strategy, which already holds the row lock when it writes.
     */
    @Version
    @Column(nullable = false)
    private long version;

    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt;

    protected Account() {
        // for JPA
    }

    private Account(AccountKind kind) {
        this.kind = kind;
        this.createdAt = Instant.now();
    }

    public static Account openCustomer() {
        return new Account(AccountKind.CUSTOMER);
    }

    public void debit(long cents) {
        if (kind == AccountKind.CUSTOMER && balanceCents < cents) {
            throw new InsufficientFundsException(id);
        }
        balanceCents = Math.subtractExact(balanceCents, cents);
    }

    public void credit(long cents) {
        balanceCents = Math.addExact(balanceCents, cents);
    }

    public boolean isCustomer() {
        return kind == AccountKind.CUSTOMER;
    }

    public UUID getId() {
        return id;
    }

    public AccountKind getKind() {
        return kind;
    }

    public long getBalanceCents() {
        return balanceCents;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }
}
