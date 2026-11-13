package tech.lucassantana.ledger.transfer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.UUID;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.test.context.TestPropertySource;
import tech.lucassantana.ledger.TestcontainersConfiguration;
import tech.lucassantana.ledger.account.Account;
import tech.lucassantana.ledger.account.AccountNotFoundException;
import tech.lucassantana.ledger.account.AccountRepository;
import tech.lucassantana.ledger.account.InsufficientFundsException;
import tech.lucassantana.ledger.money.Money;

/**
 * Every strategy answers the failure cases the same way and writes nothing when it refuses.
 * {@link TransferApiTest} checks the HTTP mapping on the default strategy; this checks that each
 * strategy raises the exceptions behind it, in both lock orders (source id smaller and larger).
 */
@SpringBootTest
@Import(TestcontainersConfiguration.class)
class StrategyErrorPathsTest {

    @Nested
    @TestPropertySource(properties = "ledger.concurrency.strategy=pessimistic")
    class Pessimistic extends ErrorPaths {
    }

    @Nested
    @TestPropertySource(properties = "ledger.concurrency.strategy=conditional-update")
    class ConditionalUpdate extends ErrorPaths {
    }

    @Nested
    @TestPropertySource(properties = "ledger.concurrency.strategy=optimistic")
    class Optimistic extends ErrorPaths {
    }

    abstract static class ErrorPaths {

        @Autowired
        TransferService service;

        @Autowired
        AccountRepository accounts;

        @Autowired
        JdbcClient jdbc;

        @Test
        void insufficientFundsWritesNothing() {
            for (boolean sourceFirst : new boolean[] {true, false}) {
                UUID[] pair = orderedPair(sourceFirst);
                UUID from = pair[0];
                UUID to = pair[1];
                service.deposit(key(), from, new Money(1_000));

                assertThatThrownBy(() -> service.transfer(key(), from, to, new Money(1_001)))
                        .isInstanceOf(InsufficientFundsException.class);

                assertThat(balance(from)).isEqualTo(1_000);
                assertThat(balance(to)).isZero();
                assertThat(transfersFrom(from)).isZero();
            }
        }

        @Test
        void unknownSourceOrDestinationIs404() {
            for (boolean sourceFirst : new boolean[] {true, false}) {
                UUID existing = funded();
                UUID missing = missingIdOn(sourceFirst ? Side.AFTER : Side.BEFORE, existing);

                assertThatThrownBy(() -> service.transfer(key(), missing, existing, new Money(100)))
                        .isInstanceOf(AccountNotFoundException.class);
                assertThatThrownBy(() -> service.transfer(key(), existing, missing, new Money(100)))
                        .isInstanceOf(AccountNotFoundException.class);
                assertThat(balance(existing)).isEqualTo(10_000);
            }
        }

        @Test
        void settlementAccountCannotBeUsedDirectly() {
            UUID customer = funded();

            assertThatThrownBy(() -> service.transfer(key(), customer, Account.SETTLEMENT_ID, new Money(100)))
                    .isInstanceOf(InvalidTransferException.class);
            assertThatThrownBy(() -> service.transfer(key(), Account.SETTLEMENT_ID, customer, new Money(100)))
                    .isInstanceOf(InvalidTransferException.class);
            assertThat(balance(customer)).isEqualTo(10_000);
        }

        @Test
        void exactBalanceCanBeSpent() {
            UUID from = funded();
            UUID to = accounts.save(Account.openCustomer()).getId();

            service.transfer(key(), from, to, new Money(10_000));

            assertThat(balance(from)).isZero();
            assertThat(balance(to)).isEqualTo(10_000);
        }

        private enum Side { BEFORE, AFTER }

        /** Two fresh customer accounts, returned as {source, destination} with the source id smaller or larger. */
        private UUID[] orderedPair(boolean sourceFirst) {
            UUID a = accounts.save(Account.openCustomer()).getId();
            UUID b = accounts.save(Account.openCustomer()).getId();
            UUID smaller = a.compareTo(b) < 0 ? a : b;
            UUID larger = smaller == a ? b : a;
            return sourceFirst ? new UUID[] {smaller, larger} : new UUID[] {larger, smaller};
        }

        /** An id that does not exist and sorts before or after {@code other}, so both lock orders are covered. */
        private static UUID missingIdOn(Side side, UUID other) {
            UUID candidate;
            do {
                candidate = UUID.randomUUID();
            } while ((candidate.compareTo(other) < 0) != (side == Side.BEFORE));
            return candidate;
        }

        private UUID funded() {
            UUID id = accounts.save(Account.openCustomer()).getId();
            service.deposit(key(), id, new Money(10_000));
            return id;
        }

        private long balance(UUID id) {
            return jdbc.sql("select balance_cents from accounts where id = ?").param(id).query(Long.class).single();
        }

        private long transfersFrom(UUID id) {
            return jdbc.sql("select count(*) from transfers where from_account_id = ?").param(id).query(Long.class).single();
        }

        private static String key() {
            return UUID.randomUUID().toString();
        }
    }
}
