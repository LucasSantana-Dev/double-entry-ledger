package tech.lucassantana.ledger.transfer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.jayway.jsonpath.JsonPath;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.dao.DataAccessException;
import org.springframework.http.MediaType;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.ResultActions;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.web.context.WebApplicationContext;
import tech.lucassantana.ledger.TestcontainersConfiguration;
import tech.lucassantana.ledger.account.Account;
import tech.lucassantana.ledger.idempotency.IdempotencyKeyRepository;

@SpringBootTest
@Import(TestcontainersConfiguration.class)
class TransferApiTest {

    @Autowired
    WebApplicationContext context;

    @Autowired
    JdbcClient jdbc;

    @Autowired
    IdempotencyKeyRepository keys;

    @Autowired
    TransactionTemplate transaction;

    MockMvc mvc;

    @BeforeEach
    void setUp() {
        mvc = MockMvcBuilders.webAppContextSetup(context).build();
    }

    @Test
    void depositAndTransferMoveMoneyBetweenAccounts() throws Exception {
        UUID alice = openAccount();
        UUID bob = openAccount();

        deposit(alice, "100.00", newKey()).andExpect(status().isCreated());
        transfer(alice, bob, "30.00", newKey())
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.amount").value("30.00"));

        assertBalance(alice, "70.00");
        assertBalance(bob, "30.00");
    }

    @Test
    void retryWithTheSameKeyReplaysWithoutMovingMoneyAgain() throws Exception {
        UUID alice = funded("100.00");
        UUID bob = openAccount();
        String key = newKey();

        String first = transfer(alice, bob, "10.00", key)
                .andExpect(status().isCreated())
                .andExpect(header().doesNotExist("Idempotent-Replayed"))
                .andReturn().getResponse().getContentAsString();
        transfer(alice, bob, "10.00", key)
                .andExpect(status().isCreated())
                .andExpect(header().string("Idempotent-Replayed", "true"))
                .andExpect(jsonPath("$.id").value(JsonPath.<String>read(first, "$.id")));

        assertBalance(alice, "90.00");
    }

    @Test
    void reusingAKeyWithADifferentPayloadIsRejected() throws Exception {
        UUID alice = funded("100.00");
        UUID bob = openAccount();
        String key = newKey();

        transfer(alice, bob, "10.00", key).andExpect(status().isCreated());
        transfer(alice, bob, "11.00", key)
                .andExpect(status().is(422))
                .andExpect(jsonPath("$.type").value("/problems/idempotency-key-reused"));

        assertBalance(alice, "90.00");
    }

    @Test
    void keyStillInUseByARunningRequestGets409() throws Exception {
        UUID alice = funded("100.00");
        UUID bob = openAccount();
        String key = newKey();
        CountDownLatch held = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);

        // Another request holds the key: its transaction took the advisory lock and has not finished.
        CompletableFuture<Void> running = CompletableFuture.runAsync(() -> transaction.executeWithoutResult(status -> {
            assertThat(keys.tryLock(key)).isTrue();
            held.countDown();
            await(release);
        }));
        await(held);
        try {
            transfer(alice, bob, "10.00", key)
                    .andExpect(status().isConflict())
                    .andExpect(jsonPath("$.type").value("/problems/idempotency-key-in-progress"));
        } finally {
            release.countDown();
            running.get(10, TimeUnit.SECONDS);
        }

        transfer(alice, bob, "10.00", key).andExpect(status().isCreated());
    }

    @Test
    void insufficientFundsRollsBackAndReleasesTheKey() throws Exception {
        UUID alice = openAccount();
        UUID bob = openAccount();
        String key = newKey();

        transfer(alice, bob, "5.00", key)
                .andExpect(status().is(422))
                .andExpect(jsonPath("$.type").value("/problems/insufficient-funds"));

        deposit(alice, "10.00", newKey()).andExpect(status().isCreated());
        transfer(alice, bob, "5.00", key).andExpect(status().isCreated());
        assertBalance(alice, "5.00");
    }

    @Test
    void unknownAccountIs404() throws Exception {
        transfer(funded("1.00"), UUID.randomUUID(), "1.00", newKey())
                .andExpect(status().isNotFound())
                .andExpect(jsonPath("$.type").value("/problems/account-not-found"));
    }

    @Test
    void settlementAccountIsInternal() throws Exception {
        UUID alice = openAccount();

        transfer(Account.SETTLEMENT_ID, alice, "1.00", newKey())
                .andExpect(status().is(422))
                .andExpect(jsonPath("$.type").value("/problems/invalid-transfer"));
        mvc.perform(get("/accounts/{id}", Account.SETTLEMENT_ID)).andExpect(status().isNotFound());
    }

    @Test
    void transferToTheSameAccountIsRejected() throws Exception {
        UUID alice = funded("10.00");

        transfer(alice, alice, "1.00", newKey())
                .andExpect(status().is(422))
                .andExpect(jsonPath("$.type").value("/problems/invalid-transfer"));
    }

    @Test
    void amountOutsideThePixPatternIs400() throws Exception {
        transfer(openAccount(), openAccount(), "10", newKey())
                .andExpect(status().isBadRequest())
                .andExpect(jsonPath("$.type").value("/problems/invalid-amount"));
    }

    @Test
    void missingIdempotencyKeyIs400() throws Exception {
        mvc.perform(post("/transfers")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(body(openAccount(), openAccount(), "1.00")))
                .andExpect(status().isBadRequest());
    }

    @Test
    void entriesAreAppendOnly() throws Exception {
        UUID alice = funded("10.00");

        assertThatThrownBy(() -> jdbc.sql("update entries set amount_cents = 1 where account_id = ?")
                        .param(alice).update())
                .isInstanceOf(DataAccessException.class)
                .hasMessageContaining("append-only");
        assertThatThrownBy(() -> jdbc.sql("delete from entries where account_id = ?").param(alice).update())
                .isInstanceOf(DataAccessException.class)
                .hasMessageContaining("append-only");
    }

    private UUID openAccount() throws Exception {
        String body = mvc.perform(post("/accounts"))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.balance").value("0.00"))
                .andReturn().getResponse().getContentAsString();
        return UUID.fromString(JsonPath.read(body, "$.id"));
    }

    private UUID funded(String amount) throws Exception {
        UUID account = openAccount();
        deposit(account, amount, newKey()).andExpect(status().isCreated());
        return account;
    }

    private ResultActions transfer(UUID from, UUID to, String amount, String key) throws Exception {
        return mvc.perform(post("/transfers")
                .header("Idempotency-Key", key)
                .contentType(MediaType.APPLICATION_JSON)
                .content(body(from, to, amount)));
    }

    private ResultActions deposit(UUID account, String amount, String key) throws Exception {
        return mvc.perform(post("/deposits")
                .header("Idempotency-Key", key)
                .contentType(MediaType.APPLICATION_JSON)
                .content("""
                        {"accountId":"%s","amount":"%s"}""".formatted(account, amount)));
    }

    private void assertBalance(UUID account, String expected) throws Exception {
        mvc.perform(get("/accounts/{id}", account))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.balance").value(expected));
    }

    private static String body(UUID from, UUID to, String amount) {
        return """
                {"fromAccountId":"%s","toAccountId":"%s","amount":"%s"}""".formatted(from, to, amount);
    }

    private static String newKey() {
        return UUID.randomUUID().toString();
    }

    private static void await(CountDownLatch latch) {
        try {
            if (!latch.await(10, TimeUnit.SECONDS)) {
                throw new IllegalStateException("timed out waiting for the other transaction");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }
}
