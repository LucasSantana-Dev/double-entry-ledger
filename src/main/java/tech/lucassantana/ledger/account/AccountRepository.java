package tech.lucassantana.ledger.account;

import jakarta.persistence.LockModeType;
import java.util.Optional;
import java.util.UUID;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;

public interface AccountRepository extends JpaRepository<Account, UUID> {

    /** Row lock held until commit, so concurrent transfers on this account run one at a time (ADR 0007). */
    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("select a from Account a where a.id = :id")
    Optional<Account> findByIdForUpdate(UUID id);

    /**
     * Debits without reading the balance first: the condition is checked by the same statement
     * that writes, under the row lock it takes (conditional-update strategy, ADR 0010). Returns 0
     * when the account is missing, is the settlement account while that is not allowed, or is a
     * customer holding less than {@code cents}.
     */
    @Modifying
    @Query(value = """
            update accounts set balance_cents = balance_cents - :cents
            where id = :id
              and ((kind = 'CUSTOMER' and balance_cents >= :cents) or (:settlementAllowed and kind = 'SETTLEMENT'))
            """, nativeQuery = true)
    int debitIfCovered(UUID id, long cents, boolean settlementAllowed);

    /** Credits a customer account. Returns 0 when the account is missing or is the settlement account. */
    @Modifying
    @Query(value = """
            update accounts set balance_cents = balance_cents + :cents
            where id = :id and kind = 'CUSTOMER'
            """, nativeQuery = true)
    int creditCustomer(UUID id, long cents);
}
