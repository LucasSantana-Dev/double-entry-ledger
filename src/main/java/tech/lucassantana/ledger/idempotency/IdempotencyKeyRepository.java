package tech.lucassantana.ledger.idempotency;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;

public interface IdempotencyKeyRepository extends JpaRepository<IdempotencyKey, String> {

    /**
     * Non-blocking, transaction-scoped lock on the key. False means another request with the
     * same key is still running, which the API answers with 409 (ADR 0006). Released on commit
     * or rollback, so a crash never leaves a key stuck.
     */
    @Query(value = "select pg_try_advisory_xact_lock(hashtextextended(:key, 0))", nativeQuery = true)
    boolean tryLock(String key);
}
