package tech.lucassantana.ledger.transfer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.jayway.jsonpath.JsonPath;
import io.micrometer.core.instrument.MeterRegistry;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.dao.CannotAcquireLockException;
import org.springframework.http.MediaType;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.ResultActions;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.context.WebApplicationContext;
import tech.lucassantana.ledger.TestcontainersConfiguration;
import tech.lucassantana.ledger.account.InsufficientFundsException;

/**
 * Bounded retry and the 503 answer (ADR 0010), with a strategy stub that conflicts on demand.
 * The real strategies are exercised under real contention by {@link ConcurrentTransfersInvariantTest}.
 */
@SpringBootTest
@Import(TestcontainersConfiguration.class)
class TransferRetryTest {

    @MockitoBean
    ConcurrencyStrategy strategy;

    @Autowired
    WebApplicationContext context;

    @Autowired
    JdbcClient jdbc;

    @Autowired
    MeterRegistry meters;

    MockMvc mvc;
    UUID from;
    UUID to;

    @BeforeEach
    void setUp() throws Exception {
        mvc = MockMvcBuilders.webAppContextSetup(context).build();
        from = openAccount();
        to = openAccount();
    }

    @Test
    void conflictOnEveryAttemptAnswers503AndWritesNothing() throws Exception {
        doThrow(new CannotAcquireLockException("conflict"))
                .when(strategy).move(any(), any(), anyLong(), anyBoolean());
        double exhaustedBefore = counter("ledger.transfer.conflict.exhausted");
        double retriesBefore = counter("ledger.transfer.conflict.retries");
        String key = UUID.randomUUID().toString();

        transfer(key)
                .andExpect(status().isServiceUnavailable())
                .andExpect(header().string("Retry-After", "1"))
                .andExpect(jsonPath("$.type").value("/problems/concurrency-conflict"));

        verify(strategy, times(TransferService.MAX_ATTEMPTS)).move(any(), any(), anyLong(), anyBoolean());
        assertThat(counter("ledger.transfer.conflict.exhausted") - exhaustedBefore).isEqualTo(1);
        assertThat(counter("ledger.transfer.conflict.retries") - retriesBefore)
                .isEqualTo(TransferService.MAX_ATTEMPTS - 1);
        assertThat(rows("select count(*) from idempotency_keys where idempotency_key = ?", key)).isZero();
        assertThat(rows("select count(*) from transfers where from_account_id = ?", from)).isZero();

        // The key holds nothing, so the client's retry with it goes through once conflicts stop.
        doNothing().when(strategy).move(any(), any(), anyLong(), anyBoolean());
        transfer(key).andExpect(status().isCreated()).andExpect(header().doesNotExist("Idempotent-Replayed"));
    }

    @Test
    void conflictThatClearsBeforeTheLastAttemptSucceeds() throws Exception {
        doThrow(new CannotAcquireLockException("conflict"))
                .doThrow(new CannotAcquireLockException("conflict"))
                .doNothing()
                .when(strategy).move(any(), any(), anyLong(), anyBoolean());

        transfer(UUID.randomUUID().toString()).andExpect(status().isCreated());

        verify(strategy, times(3)).move(any(), any(), anyLong(), anyBoolean());
        assertThat(rows("select count(*) from transfers where from_account_id = ?", from)).isEqualTo(1);
    }

    @Test
    void businessFailureIsAFinalAnswerAndIsNotRetried() throws Exception {
        doThrow(new InsufficientFundsException(from)).when(strategy).move(any(), any(), anyLong(), anyBoolean());

        transfer(UUID.randomUUID().toString())
                .andExpect(status().is(422))
                .andExpect(jsonPath("$.type").value("/problems/insufficient-funds"));

        verify(strategy, times(1)).move(any(), any(), anyLong(), anyBoolean());
    }

    private ResultActions transfer(String key) throws Exception {
        return mvc.perform(post("/transfers")
                .header("Idempotency-Key", key)
                .contentType(MediaType.APPLICATION_JSON)
                .content("""
                        {"fromAccountId":"%s","toAccountId":"%s","amount":"10.00"}
                        """.formatted(from, to)));
    }

    private UUID openAccount() throws Exception {
        String body = mvc.perform(post("/accounts")).andExpect(status().isCreated())
                .andReturn().getResponse().getContentAsString();
        return UUID.fromString(JsonPath.read(body, "$.id"));
    }

    private double counter(String name) {
        return meters.counter(name).count();
    }

    private long rows(String sql, Object param) {
        return jdbc.sql(sql).param(param).query(Long.class).single();
    }
}
