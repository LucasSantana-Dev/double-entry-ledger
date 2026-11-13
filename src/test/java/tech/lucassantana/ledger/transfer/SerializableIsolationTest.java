package tech.lucassantana.ledger.transfer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;

import java.util.List;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;
import tech.lucassantana.ledger.TestcontainersConfiguration;
import tech.lucassantana.ledger.account.Account;
import tech.lucassantana.ledger.account.AccountRepository;
import tech.lucassantana.ledger.money.Money;

/**
 * The serializable strategy really runs at {@code SERIALIZABLE}. The invariant test alone cannot
 * tell: with the {@code @Version} column, a transfer silently left at read committed would still
 * pass, behaving like the optimistic strategy.
 */
@SpringBootTest
@Import(TestcontainersConfiguration.class)
@TestPropertySource(properties = "ledger.concurrency.strategy=serializable")
class SerializableIsolationTest {

    @MockitoSpyBean
    SerializableStrategy strategy;

    @Autowired
    TransferService service;

    @Autowired
    AccountRepository accounts;

    @Autowired
    JdbcClient jdbc;

    @Test
    void transfersRunSerializableAndLeaveTheConnectionAtTheDefault() {
        List<String> seenInsideMove = new CopyOnWriteArrayList<>();
        doAnswer(call -> {
            // JdbcClient joins the JPA transaction's connection, so this is the transfer's level.
            seenInsideMove.add(isolation());
            return call.callRealMethod();
        }).when(strategy).move(any(), any(), anyLong(), anyBoolean());

        UUID from = accounts.save(Account.openCustomer()).getId();
        UUID to = accounts.save(Account.openCustomer()).getId();
        service.deposit(UUID.randomUUID().toString(), from, new Money(1_000));
        service.transfer(UUID.randomUUID().toString(), from, to, new Money(400));

        assertThat(seenInsideMove).containsExactly("serializable", "serializable");
        assertThat(isolation()).isEqualTo("read committed");
    }

    private String isolation() {
        return jdbc.sql("show transaction_isolation").query(String.class).single();
    }
}
