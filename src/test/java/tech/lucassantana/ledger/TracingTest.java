package tech.lucassantana.ledger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import com.jayway.jsonpath.JsonPath;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.trace.SpanKind;
import io.opentelemetry.sdk.common.CompletableResultCode;
import io.opentelemetry.sdk.trace.SpanProcessor;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.opentelemetry.sdk.trace.export.SimpleSpanProcessor;
import io.opentelemetry.sdk.trace.export.SpanExporter;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.time.Duration;
import java.util.Collection;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.micrometer.tracing.test.autoconfigure.AutoConfigureTracing;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.SpringBootTest.WebEnvironment;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.http.MediaType;
import org.springframework.jdbc.core.simple.JdbcClient;
import org.springframework.web.client.RestClient;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;

/**
 * Criteria 1 to 3 of phase 4 (ADR 0021, ADR 0023): one transfer is one trace from the HTTP
 * request through the outbox to the statement consumer, the Kafka record carries the context,
 * and tracing never holds an event back. Spans are captured in process, from the real
 * OpenTelemetry SDK that Boot configures, by an extra span processor. Requests go over real HTTP,
 * so the server span comes from the same filter as in production.
 *
 * <p>{@code @AutoConfigureTracing} undoes Boot's test default {@code management.tracing.export.enabled=false},
 * which also switches off the W3C propagators, so no context would reach the outbox row or the
 * Kafka header. Nothing leaves the JVM: without an OTLP endpoint Boot creates no span exporter.
 */
@SpringBootTest(webEnvironment = WebEnvironment.RANDOM_PORT)
@AutoConfigureTracing
@Import({TestcontainersConfiguration.class, TracingTest.SpanCapture.class})
class TracingTest {

    private static final Duration WAIT = Duration.ofSeconds(30);
    private static final AttributeKey<String> SEQUENCE = AttributeKey.stringKey("ledger.outbox.sequence");

    /**
     * Collects every ended span, synchronously. Boot adds each {@code SpanProcessor} bean to its
     * tracer provider. The exporter is deliberately not a bean: Boot would also hand a
     * {@code SpanExporter} bean to its batch processor, and every span would arrive twice.
     */
    @TestConfiguration(proxyBeanMethods = false)
    static class SpanCapture {

        @Bean
        CapturedSpans capturedSpans() {
            return new CapturedSpans();
        }

        @Bean
        SpanProcessor captureProcessor(CapturedSpans spans) {
            return SimpleSpanProcessor.create(new SpanExporter() {

                @Override
                public CompletableResultCode export(Collection<SpanData> batch) {
                    spans.spans.addAll(batch);
                    return CompletableResultCode.ofSuccess();
                }

                @Override
                public CompletableResultCode flush() {
                    return CompletableResultCode.ofSuccess();
                }

                @Override
                public CompletableResultCode shutdown() {
                    return CompletableResultCode.ofSuccess();
                }
            });
        }
    }

    static final class CapturedSpans {

        private final List<SpanData> spans = new CopyOnWriteArrayList<>();

        List<SpanData> inTrace(String traceId) {
            return spans.stream().filter(s -> s.getTraceId().equals(traceId)).toList();
        }

        List<SpanData> all() {
            return List.copyOf(spans);
        }
    }

    /** A W3C traceparent split into its fields. */
    private record TraceParent(String traceId, String spanId, boolean sampled) {

        static TraceParent parse(String header) {
            String[] parts = header.split("-");
            assertThat(parts).as("traceparent %s", header).hasSize(4);
            assertThat(parts[0]).isEqualTo("00");
            // Bit 0 of the flags is "sampled"; OpenTelemetry also sets bit 1 ("random trace id").
            return new TraceParent(parts[1], parts[2], (HexFormat.fromHexDigits(parts[3]) & 1) == 1);
        }
    }

    @LocalServerPort
    private int port;
    @Autowired
    private CapturedSpans spans;
    @Autowired
    private JdbcClient jdbc;
    @Autowired
    private KafkaContainer kafka;
    @Autowired
    private PostgreSQLContainer postgres;
    @Value("${ledger.outbox.topic}")
    private String topic;

    @Test
    void oneTransferIsOneTraceFromTheRequestToTheStatementLine() {
        UUID alice = openAccount();
        UUID bob = openAccount();
        deposit(alice, "100.00");

        UUID transfer = transfer(alice, bob, "25.00", Optional.empty());

        TraceParent stored = TraceParent.parse(storedTraceparent(transfer));
        assertThat(stored.sampled()).isTrue();
        awaitStatementLines(transfer);

        // Criterion 1: HTTP server span, JDBC spans, relay send span, Kafka producer and consumer
        // spans, all in one trace and chained in that order.
        await().atMost(WAIT).untilAsserted(() -> {
            List<SpanData> trace = spans.inTrace(stored.traceId());
            Map<String, SpanData> byId = trace.stream().collect(Collectors.toMap(SpanData::getSpanId, s -> s));
            String described = describe(trace);

            SpanData server = single(trace, s -> s.getKind() == SpanKind.SERVER, "HTTP server span", described);
            assertThat(server.getParentSpanContext().isValid()).as("server span is the root").isFalse();
            assertThat(server.getName()).as(described).contains("/transfers");
            assertThat(trace).as("JDBC spans in %s", described)
                    .anyMatch(s -> s.getName().contains("query") && isDescendant(s, server, byId));

            // The span the outbox row stored belongs to the request.
            SpanData writer = byId.get(stored.spanId());
            assertThat(writer).as("stored span %s in %s", stored.spanId(), described).isNotNull();
            assertThat(writer == server || isDescendant(writer, server, byId)).as(described).isTrue();

            SpanData relay = single(trace, s -> s.getName().equals("outbox relay"), "relay span", described);
            assertThat(relay.getParentSpanId()).as("relay parent is the stored context").isEqualTo(stored.spanId());
            SpanData send = single(trace, s -> s.getKind() == SpanKind.PRODUCER, "Kafka send span", described);
            assertThat(send.getParentSpanId()).as("send span parent").isEqualTo(relay.getSpanId());
            SpanData receive = single(trace, s -> s.getKind() == SpanKind.CONSUMER, "Kafka consumer span", described);
            assertThat(receive.getParentSpanId()).as("consumer span parent").isEqualTo(send.getSpanId());
            // The statement's own SQL runs inside the consumer span.
            assertThat(trace).as("statement SQL in %s", described)
                    .anyMatch(s -> s.getName().contains("query") && isDescendant(s, receive, byId));
        });

        // Criterion 2: the record on the topic carries a W3C traceparent of the same trace.
        ConsumerRecord<String, String> record = recordOf(transfer);
        Header header = record.headers().lastHeader("traceparent");
        assertThat(header).as("traceparent header").isNotNull();
        TraceParent sent = TraceParent.parse(new String(header.value(), StandardCharsets.UTF_8));
        assertThat(sent.traceId()).isEqualTo(stored.traceId());
        assertThat(sent.spanId()).as("header names the send span")
                .isEqualTo(single(spans.inTrace(stored.traceId()), s -> s.getKind() == SpanKind.PRODUCER,
                        "Kafka send span", "").getSpanId());
    }

    @Test
    void aTransferCommittedWhileNotSampledIsStillPublishedAndApplied() {
        UUID alice = openAccount();
        UUID bob = openAccount();
        deposit(alice, "100.00");
        // The caller decided not to sample; the parent-based sampler follows it.
        String traceId = randomHex(16);
        String unsampled = "00-" + traceId + "-" + randomHex(8) + "-00";

        UUID transfer = transfer(alice, bob, "10.00", Optional.of(unsampled));

        TraceParent stored = TraceParent.parse(storedTraceparent(transfer));
        assertThat(stored.traceId()).isEqualTo(traceId);
        assertThat(stored.sampled()).isFalse();
        awaitStatementLines(transfer);
        Header header = recordOf(transfer).headers().lastHeader("traceparent");
        assertThat(header).isNotNull();
        TraceParent sent = TraceParent.parse(new String(header.value(), StandardCharsets.UTF_8));
        assertThat(sent.traceId()).isEqualTo(traceId);
        assertThat(sent.sampled()).isFalse();
        assertThat(spans.inTrace(traceId)).as("nothing recorded for an unsampled trace").isEmpty();
    }

    @Test
    void anOutboxRowWithoutTraceContextIsSentInANewTrace() throws Exception {
        UUID alice = openAccount();
        UUID bob = openAccount();
        deposit(alice, "100.00");
        awaitOutboxDrained();
        UUID transfer;
        // Hold the relay off while the row loses its context, as a row written before V5 would be.
        try (Connection direct = DriverManager.getConnection(
                postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
                Statement sql = direct.createStatement()) {
            sql.execute("select pg_advisory_lock(4242, 1)");
            transfer = transfer(alice, bob, "5.00", Optional.empty());
            assertThat(jdbc.sql("update outbox_events set traceparent = null where aggregate_id = ?")
                    .param(transfer).update()).isOne();
            sql.execute("select pg_advisory_unlock(4242, 1)");
        }

        awaitStatementLines(transfer);
        String sequence = String.valueOf(jdbc.sql("select id from outbox_events where aggregate_id = ?")
                .param(transfer).query(Long.class).single());
        await().atMost(WAIT).untilAsserted(() -> {
            // At least once delivery (ADR 0016): a row may be sent more than once, each in its own span.
            List<SpanData> relays = spans.all().stream()
                    .filter(s -> sequence.equals(s.getAttributes().get(SEQUENCE)))
                    .toList();
            assertThat(relays).as("relay spans of sequence %s", sequence).isNotEmpty()
                    .allSatisfy(relay -> assertThat(relay.getParentSpanContext().isValid())
                            .as("relay span is a new root").isFalse());
            assertThat(relays).as("one relay span leads to the statement consumer").anySatisfy(relay -> {
                List<SpanData> trace = spans.inTrace(relay.getTraceId());
                String described = describe(trace);
                SpanData send = single(trace, s -> s.getKind() == SpanKind.PRODUCER, "Kafka send span", described);
                assertThat(send.getParentSpanId()).isEqualTo(relay.getSpanId());
                SpanData receive = single(trace, s -> s.getKind() == SpanKind.CONSUMER, "Kafka consumer span",
                        described);
                assertThat(receive.getParentSpanId()).isEqualTo(send.getSpanId());
            });
        });
    }

    private UUID openAccount() {
        String body = http().post().uri("/accounts").retrieve().body(String.class);
        return UUID.fromString(JsonPath.read(body, "$.id"));
    }

    private void deposit(UUID account, String amount) {
        http().post().uri("/deposits")
                .header("Idempotency-Key", UUID.randomUUID().toString())
                .contentType(MediaType.APPLICATION_JSON)
                .body("""
                        {"accountId": "%s", "amount": "%s"}""".formatted(account, amount))
                .retrieve().toBodilessEntity();
    }

    private UUID transfer(UUID from, UUID to, String amount, Optional<String> traceparent) {
        String body = http().post().uri("/transfers")
                .header("Idempotency-Key", UUID.randomUUID().toString())
                .headers(h -> traceparent.ifPresent(value -> h.set("traceparent", value)))
                .contentType(MediaType.APPLICATION_JSON)
                .body("""
                        {"fromAccountId": "%s", "toAccountId": "%s", "amount": "%s"}""".formatted(from, to, amount))
                .retrieve().body(String.class);
        return UUID.fromString(JsonPath.read(body, "$.id"));
    }

    private RestClient http() {
        return RestClient.create("http://localhost:" + port);
    }

    private String storedTraceparent(UUID transfer) {
        String value = jdbc.sql("select traceparent from outbox_events where aggregate_id = ?")
                .param(transfer).query(String.class).single();
        assertThat(value).as("traceparent stored with the outbox row").isNotNull();
        return value;
    }

    private void awaitStatementLines(UUID transfer) {
        await().atMost(WAIT).until(() -> jdbc.sql("select count(*) from statement.statement_lines where transfer_id = ?")
                .param(transfer).query(Long.class).single() == 2);
    }

    private void awaitOutboxDrained() {
        await().atMost(WAIT).until(() -> jdbc.sql("select count(*) from outbox_events where published_at is null")
                .query(Long.class).single() == 0);
    }

    private ConsumerRecord<String, String> recordOf(UUID transfer) {
        Predicate<ConsumerRecord<String, String>> ofTransfer = r -> transfer.toString().equals(r.key());
        return TopicReader.readUntil(kafka.getBootstrapServers(), topic,
                        records -> records.stream().anyMatch(ofTransfer), WAIT)
                .stream().filter(ofTransfer).findFirst().orElseThrow();
    }

    private static SpanData single(List<SpanData> spans, Predicate<SpanData> match, String what, String described) {
        List<SpanData> found = spans.stream().filter(match).toList();
        assertThat(found).as("%s in %s", what, described).hasSize(1);
        return found.getFirst();
    }

    private static boolean isDescendant(SpanData span, SpanData ancestor, Map<String, SpanData> byId) {
        for (SpanData s = byId.get(span.getParentSpanId()); s != null; s = byId.get(s.getParentSpanId())) {
            if (s.getSpanId().equals(ancestor.getSpanId())) {
                return true;
            }
        }
        return false;
    }

    private static String describe(List<SpanData> trace) {
        return trace.stream()
                .map(s -> "%s[%s %s parent=%s]".formatted(s.getName(), s.getKind(), s.getSpanId(), s.getParentSpanId()))
                .collect(Collectors.joining(", ", "{", "}"));
    }

    private static String randomHex(int bytes) {
        byte[] value = new byte[bytes];
        ThreadLocalRandom.current().nextBytes(value);
        value[0] |= 1; // never all zeros, which W3C treats as invalid
        return HexFormat.of().formatHex(value);
    }
}
