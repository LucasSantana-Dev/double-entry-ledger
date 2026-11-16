package tech.lucassantana.ledger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.jayway.jsonpath.JsonPath;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.micrometer.metrics.test.autoconfigure.AutoConfigureMetrics;
import org.springframework.boot.micrometer.tracing.test.autoconfigure.AutoConfigureTracing;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.SpringBootTest.WebEnvironment;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.MediaType;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.web.client.RestClient;
import org.testcontainers.grafana.LgtmStackContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * Phase 4 criterion 7 (ADR 0023): a transfer's trace and a ledger metric reach a real backend
 * over OTLP. The only test that starts {@code grafana/otel-lgtm}; every other check of traces and
 * meters runs in process.
 *
 * <p>Boot's test support switches metric and trace export off unless a test asks for it, and
 * application.properties switches the two OTLP exports off unless a collector is configured. This
 * test asks for both, with {@code @AutoConfigureMetrics}, {@code @AutoConfigureTracing} and the two
 * OTLP enabled properties, and {@code @ServiceConnection} points both exporters at the container. The context is closed afterwards so the container does
 * not stay up for the rest of the suite.
 */
@SpringBootTest(webEnvironment = WebEnvironment.RANDOM_PORT, properties = {
        // Off by default in application.properties, on wherever a collector exists.
        "management.otlp.metrics.export.enabled=true",
        "management.tracing.export.otlp.enabled=true",
        // Push often, so the test does not wait out the one minute default step.
        "management.otlp.metrics.export.step=1s",
        "management.opentelemetry.tracing.export.schedule-delay=200ms"})
@AutoConfigureMetrics
@AutoConfigureTracing
@Import({TestcontainersConfiguration.class, OtlpExportSmokeTest.Lgtm.class})
@DirtiesContext
class OtlpExportSmokeTest {

    private static final Logger log = LoggerFactory.getLogger(OtlpExportSmokeTest.class);
    /** Pinned like the other images: the OTLP to Prometheus name conversion depends on the collector. */
    static final DockerImageName LGTM = DockerImageName.parse("grafana/otel-lgtm:0.40.0");
    private static final Duration WAIT = Duration.ofSeconds(90);

    @TestConfiguration(proxyBeanMethods = false)
    static class Lgtm {

        @Bean
        @ServiceConnection
        LgtmStackContainer lgtmContainer() {
            return new LgtmStackContainer(LGTM);
        }
    }

    @LocalServerPort
    private int port;
    @Autowired
    private LgtmStackContainer lgtm;
    @Autowired
    private JdbcClient jdbc;

    @Test
    void aTransfersTraceAndALedgerMetricReachTheBackend() {
        RestClient app = RestClient.create("http://localhost:" + port);
        UUID alice = openAccount(app);
        UUID bob = openAccount(app);
        app.post().uri("/deposits")
                .header("Idempotency-Key", UUID.randomUUID().toString())
                .contentType(MediaType.APPLICATION_JSON)
                .body("""
                        {"accountId": "%s", "amount": "100.00"}""".formatted(alice))
                .retrieve().toBodilessEntity();
        String transfer = JsonPath.read(app.post().uri("/transfers")
                .header("Idempotency-Key", UUID.randomUUID().toString())
                .contentType(MediaType.APPLICATION_JSON)
                .body("""
                        {"fromAccountId": "%s", "toAccountId": "%s", "amount": "25.00"}""".formatted(alice, bob))
                .retrieve().body(String.class), "$.id");
        String traceId = jdbc.sql("select traceparent from outbox_events where aggregate_id = ?")
                .param(UUID.fromString(transfer)).query(String.class).single().split("-")[1];

        // Tempo answers 404 until the batch exporter has flushed and the trace is ingested.
        RestClient tempo = RestClient.create(lgtm.getTempoUrl());
        await().atMost(WAIT).pollInterval(Duration.ofSeconds(1)).ignoreExceptions().untilAsserted(() -> {
            String trace = tempo.get().uri("/api/traces/{id}", traceId).accept(MediaType.APPLICATION_JSON)
                    .retrieve().body(String.class);
            List<String> spanNames = JsonPath.read(trace, "$..spans[*].name");
            assertThat(spanNames).as("spans of trace %s in Tempo", traceId)
                    .anyMatch(name -> name.contains("/transfers"))
                    .contains("outbox relay");
        });

        // The collector converts OTLP names for Prometheus; the dashboard queries these names.
        RestClient prometheus = RestClient.create(lgtm.getPrometheusHttpUrl());
        await().atMost(WAIT).pollInterval(Duration.ofSeconds(1)).ignoreExceptions().untilAsserted(() -> {
            String body = prometheus.get().uri("/api/v1/label/__name__/values")
                    .retrieve().onStatus(HttpStatusCode::isError, (request, response) -> {
                        throw new AssertionError("Prometheus answered " + response.getStatusCode());
                    })
                    .body(String.class);
            List<String> names = JsonPath.read(body, "$.data[*]");
            List<String> ledger = names.stream().filter(name -> name.startsWith("ledger_")).sorted().toList();
            assertThat(ledger).as("ledger metrics in Prometheus").anyMatch(name -> name.startsWith("ledger_outbox_pending"));
            log.info("ledger metrics in Prometheus: {}", ledger);
        });
    }

    private static UUID openAccount(RestClient app) {
        return UUID.fromString(JsonPath.read(app.post().uri("/accounts").retrieve().body(String.class), "$.id"));
    }
}
