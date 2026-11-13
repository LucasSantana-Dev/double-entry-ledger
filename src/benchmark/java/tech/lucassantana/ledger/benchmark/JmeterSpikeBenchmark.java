package tech.lucassantana.ledger.benchmark;

import static org.assertj.core.api.Assertions.assertThat;
import static us.abstracta.jmeter.javadsl.JmeterDsl.httpSampler;
import static us.abstracta.jmeter.javadsl.JmeterDsl.jsr223PreProcessor;
import static us.abstracta.jmeter.javadsl.JmeterDsl.testPlan;
import static us.abstracta.jmeter.javadsl.JmeterDsl.threadGroup;

import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.http.entity.ContentType;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.simple.JdbcClient;
import tech.lucassantana.ledger.TestcontainersConfiguration;
import tech.lucassantana.ledger.account.Account;
import tech.lucassantana.ledger.account.AccountRepository;
import tech.lucassantana.ledger.money.Money;
import tech.lucassantana.ledger.transfer.TransferService;
import us.abstracta.jmeter.javadsl.core.TestPlanStats;

/** Spike for ADR 0011: does jmeter-java-dsl run on Java 25 against this Boot 4 app? */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Import(TestcontainersConfiguration.class)
class JmeterSpikeBenchmark {

    @LocalServerPort
    int port;

    @Autowired
    AccountRepository accounts;

    @Autowired
    TransferService service;

    @Autowired
    JdbcClient jdbc;

    @Test
    void jmeterDrivesTransfersOverHttp() throws Exception {
        UUID from = accounts.save(Account.openCustomer()).getId();
        UUID to = accounts.save(Account.openCustomer()).getId();
        service.deposit(UUID.randomUUID().toString(), from, new Money(100_000_000));

        TestPlanStats stats = testPlan(
                threadGroup(8, 50,
                        httpSampler("http://localhost:" + port + "/transfers")
                                .post("""
                                        {"fromAccountId":"%s","toAccountId":"%s","amount":"${amount}"}
                                        """.formatted(from, to), ContentType.APPLICATION_JSON)
                                .header("Idempotency-Key", "${__UUID()}")
                                // Java lambda run per request: the real plan picks accounts this way.
                                .children(jsr223PreProcessor(s -> s.vars.put(
                                        "amount", "0.0" + ThreadLocalRandom.current().nextInt(1, 10))))))
                .run();

        System.out.printf("jmeter spike on Java %s: %d samples, %d errors, p50 %s, p99 %s%n",
                Runtime.version(), stats.overall().samplesCount(), stats.overall().errorsCount(),
                stats.overall().sampleTime().median(), stats.overall().sampleTimePercentile99());
        assertThat(stats.overall().samplesCount()).isEqualTo(400);
        assertThat(stats.overall().errorsCount()).isZero();
        // The lambda really ran per request: the amounts vary.
        assertThat(jdbc.sql("select count(distinct amount_cents) from transfers where from_account_id = ?")
                .param(from).query(Long.class).single()).isGreaterThan(1);
    }
}
