package tech.lucassantana.ledger.idempotency;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.PostLoad;
import jakarta.persistence.PostPersist;
import jakarta.persistence.Table;
import jakarta.persistence.Transient;
import java.time.Instant;
import java.util.UUID;
import org.springframework.data.domain.Persistable;

/**
 * Binds a client key to the transfer it produced. Written in the same transaction as the
 * transfer, so a key exists if and only if its transfer committed (ADR 0006).
 */
@Entity
@Table(name = "idempotency_keys")
public class IdempotencyKey implements Persistable<String> {

    @Id
    @Column(name = "idempotency_key")
    private String key;

    @Column(name = "request_fingerprint", nullable = false, updatable = false)
    private String requestFingerprint;

    @Column(name = "transfer_id", nullable = false, updatable = false)
    private UUID transferId;

    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt;

    /** The id is assigned by the client, so tell Spring Data to persist instead of merge (no extra SELECT). */
    @Transient
    private boolean isNew = true;

    protected IdempotencyKey() {
        // for JPA
    }

    public IdempotencyKey(String key, String requestFingerprint, UUID transferId) {
        this.key = key;
        this.requestFingerprint = requestFingerprint;
        this.transferId = transferId;
        this.createdAt = Instant.now();
    }

    @PostLoad
    @PostPersist
    void markNotNew() {
        isNew = false;
    }

    @Override
    public String getId() {
        return key;
    }

    @Override
    public boolean isNew() {
        return isNew;
    }

    public boolean matches(String fingerprint) {
        return requestFingerprint.equals(fingerprint);
    }

    public UUID getTransferId() {
        return transferId;
    }
}
