package tech.lucassantana.ledger.benchmark;

import static org.assertj.core.api.Assertions.assertThat;

import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * The breaking-point run (spec {@code docs/specs/2026-11-16-breaking-point}, ADRs 0030 to 0032).
 *
 * <p>For each cell (conditional-update, transfers only, one hot share) and repetition, in
 * shuffled order: a fresh PostgreSQL, the application as a child JVM with the phase 2 settings,
 * 1,000 funded accounts, a discarded warm-up, then a ladder of arrival-rate steps from the
 * open-model generator ({@link OpenLoad}). After each step the generator and the server drain,
 * the ledger invariants are checked, and one JSON line goes to {@code steps.jsonl}. The ladder
 * stops after {@code stop-after-failures} steps in a row miss the SLO, or when the application
 * dies.
 *
 * <pre>
 * ./mvnw -Pbenchmark verify -Dledger.bench.include=**&#47;BreakingPointBenchmark.java
 * ./mvnw -Pbenchmark verify -Dledger.bench.include=**&#47;BreakingPointBenchmark.java \
 *        -Dledger.bench.hot-shares=0 -Dledger.bench.rates=100,200 -Dledger.bench.step=PT10S \
 *        -Dledger.bench.warmup=PT5S       # a quick smoke run
 * </pre>
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class BreakingPointBenchmark {

    static final String STRATEGY = "conditional-update";
    /** SLO of ADR 0030. */
    static final double SLO_P99_MS = 500;
    static final double SLO_ERROR_SHARE = 0.01;

    private static final List<Double> HOT_SHARES = doubles(System.getProperty("ledger.bench.hot-shares", "0,0.9"));
    private static final String RATES = System.getProperty("ledger.bench.rates", "250,500,1000,1500,2000,3000,4000");
    private static final Duration STEP = Duration.parse(System.getProperty("ledger.bench.step", "PT2M"));
    private static final Duration WARMUP = Duration.parse(System.getProperty("ledger.bench.warmup", "PT30S"));
    private static final double WARMUP_RATE = Double.parseDouble(System.getProperty("ledger.bench.warmup-rate", "250"));
    private static final Duration TIMEOUT = Duration.parse(System.getProperty("ledger.bench.timeout", "PT5S"));
    private static final int MAX_IN_FLIGHT = Integer.getInteger("ledger.bench.max-in-flight", 10_000);
    /** Consecutive steps outside the SLO after which the ladder stops; 0 climbs the whole ladder. */
    private static final int STOP_AFTER_FAILURES = Integer.getInteger("ledger.bench.stop-after-failures", 2);
    private static final int REPETITIONS = Integer.getInteger("ledger.bench.repetitions", 1);
    private static final long SEED = Long.getLong("ledger.bench.seed", 20261116L);
    private static final Path JAR = Path.of(System.getProperty("ledger.bench.jar", "target/double-entry-ledger-0.0.1-SNAPSHOT.jar"));
    private static final Path OUT = Path.of(System.getProperty("ledger.bench.out", "target/benchmark"));

    private record Run(double hotShare, int repetition) {

        String cellId() {
            return "%s-hot%02d".formatted(STRATEGY, Math.round(hotShare * 100));
        }
    }

    /**
     * Acceptance criterion 1: a stall in the generator itself (here the request builder blocks
     * for one second) shows up as latency of every request scheduled during the stall, even
     * though the server answers at once. A generator that timed from the actual send would
     * report about 1 ms for all of them.
     */
    @Test
    @Order(1)
    void generatorMeasuresFromScheduledTime() throws Exception {
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.setExecutor(Executors.newVirtualThreadPerTaskExecutor());
        server.createContext("/", exchange -> {
            exchange.sendResponseHeaders(200, -1);
            exchange.close();
        });
        server.start();
        try (HttpClient client = HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1).build()) {
            URI uri = URI.create("http://127.0.0.1:" + server.getAddress().getPort() + "/");
            AtomicInteger built = new AtomicInteger();
            // 200 req/s for 3 s; request 200 (scheduled at 1.0 s) holds the generator until 2.0 s.
            OpenLoad.Step step = OpenLoad.run(client, () -> {
                if (built.getAndIncrement() == 200) {
                    sleep(Duration.ofSeconds(1));
                }
                return HttpRequest.newBuilder(uri).GET();
            }, 200, Duration.ofSeconds(3), Duration.ofSeconds(5), 1_000);

            assertThat(step.count(s -> s == 200)).isEqualTo(600);
            long cap = TIMEOUT.toNanos();
            // Requests scheduled in [1.0 s, 2.0 s) leave at 2.0 s: a third of all requests, with
            // latencies spread from 1 s down to 0, so p90 is about 0.7 s and max about 1 s.
            assertThat(OpenLoad.percentileMs(step.latencyNanos(), 50, cap)).isLessThan(100);
            assertThat(OpenLoad.percentileMs(step.latencyNanos(), 90, cap)).isBetween(550.0, 950.0);
            assertThat(OpenLoad.percentileMs(step.latencyNanos(), 100, cap)).isBetween(900.0, 1_400.0);
            assertThat(OpenLoad.percentileMs(step.sendLagNanos(), 100, cap)).isBetween(900.0, 1_400.0);
        } finally {
            server.stop(0);
        }
    }

    @Test
    @Order(2)
    void climbLadder() throws Exception {
        assertThat(JAR).as("packaged application").exists();
        List<Run> runs = new ArrayList<>();
        for (int repetition = 1; repetition <= REPETITIONS; repetition++) {
            for (double hot : HOT_SHARES) {
                runs.add(new Run(hot, repetition));
            }
        }
        Collections.shuffle(runs, new Random(SEED));
        Files.createDirectories(OUT);
        writeRunInfo(runs.size());

        List<String> brokenLedgers = new ArrayList<>();
        int done = 0;
        for (Run run : runs) {
            System.out.printf("[%d/%d] %s rep %d%n", ++done, runs.size(), run.cellId(), run.repetition());
            brokenLedgers.addAll(climb(run));
        }
        assertThat(brokenLedgers).as("steps after which a ledger invariant failed").isEmpty();
    }

    /** One cell and repetition: warm-up, then the ladder. Returns the steps whose ledger check failed. */
    private List<String> climb(Run run) throws Exception {
        List<String> broken = new ArrayList<>();
        Cell cell = new Cell("breaking-point", STRATEGY, run.hotShare(), 0, true);
        try (PostgreSQLContainer db = new PostgreSQLContainer(DockerImageName.parse("postgres:18-alpine"))) {
            db.start();
            Path log = OUT.resolve("logs").resolve(run.cellId() + "-rep" + run.repetition() + ".log");
            try (AppProcess app = AppProcess.start(JAR, cell, db.getJdbcUrl(), db.getUsername(), db.getPassword(), log);
                    HttpClient client = HttpClient.newBuilder()
                            .version(HttpClient.Version.HTTP_1_1)
                            .connectTimeout(TIMEOUT)
                            .build()) {
                List<String> accounts = LedgerBenchmark.seed(app);
                Supplier<HttpRequest.Builder> transfers = transfers(app, accounts, run.hotShare());

                OpenLoad.run(client, transfers, WARMUP_RATE, WARMUP, TIMEOUT, MAX_IN_FLIGHT); // discarded
                drain(app);

                List<Double> rates = doubles(System.getProperty(
                        "ledger.bench.rates.hot%02d".formatted(Math.round(run.hotShare() * 100)), RATES));
                int failuresInARow = 0;
                for (int index = 0; index < rates.size(); index++) {
                    double rate = rates.get(index);
                    LedgerBenchmark.ResourceSampler sampler = LedgerBenchmark.ResourceSampler.start(app);
                    OpenLoad.Step step = OpenLoad.run(client, transfers, rate, STEP, TIMEOUT, MAX_IN_FLIGHT);
                    LedgerBenchmark.ResourceSampler.Summary resources = sampler.stop();
                    double drainSeconds = drain(app);
                    boolean alive = app.alive();
                    boolean ledgerOk = ledgerHolds(db);
                    Map<String, Object> row = row(run, index, rate, step, resources, drainSeconds, alive, ledgerOk);
                    if (!alive) {
                        row.put("exit", app.tail().contains("OutOfMemoryError") ? "out-of-memory" : "exited");
                    }
                    Files.writeString(OUT.resolve("steps.jsonl"), LedgerBenchmark.json(row) + "\n", StandardCharsets.UTF_8,
                            StandardOpenOption.CREATE, StandardOpenOption.APPEND);
                    System.out.printf("  %5.0f req/s: %s created/s, p99 %s ms, errors %s, slo %s, ledger %s%n", rate,
                            row.get("createdPerSecond"), row.get("p99Ms"), row.get("errorShare"), row.get("sloPass"),
                            ledgerOk ? "ok" : "BROKEN");
                    if (!ledgerOk) {
                        broken.add(run.cellId() + " rep " + run.repetition() + " at " + rate + " req/s");
                    }
                    failuresInARow = (boolean) row.get("sloPass") ? 0 : failuresInARow + 1;
                    if (!alive || (STOP_AFTER_FAILURES > 0 && failuresInARow >= STOP_AFTER_FAILURES)) {
                        break;
                    }
                }
            }
        }
        return broken;
    }

    /** Transfers as in phase 2 (ADR 0012): the hot account on one side with probability {@code hotShare}. */
    private static Supplier<HttpRequest.Builder> transfers(AppProcess app, List<String> ids, double hotShare) {
        URI uri = URI.create(app.baseUrl + "/transfers");
        String hot = ids.getFirst();
        int n = ids.size();
        return () -> {
            ThreadLocalRandom random = ThreadLocalRandom.current();
            String from;
            String to;
            if (random.nextDouble() < hotShare) {
                String other = ids.get(1 + random.nextInt(n - 1));
                boolean outgoing = random.nextBoolean();
                from = outgoing ? hot : other;
                to = outgoing ? other : hot;
            } else {
                int a = random.nextInt(n);
                int b = random.nextInt(n - 1);
                from = ids.get(a);
                to = ids.get(b >= a ? b + 1 : b);
            }
            int cents = random.nextInt(1, 101);
            String body = "{\"fromAccountId\":\"%s\",\"toAccountId\":\"%s\",\"amount\":\"%d.%02d\"}"
                    .formatted(from, to, cents / 100, cents % 100);
            return HttpRequest.newBuilder(uri)
                    .header("Content-Type", "application/json")
                    .header("Idempotency-Key", UUID.randomUUID().toString())
                    .POST(HttpRequest.BodyPublishers.ofString(body));
        };
    }

    /**
     * Waits until the server has no request in progress and no thread waiting for a connection,
     * so work abandoned by timed-out clients does not spill into the next step. Returns the
     * seconds it took; gives up after 2 minutes or when the application is gone.
     */
    private static double drain(AppProcess app) throws InterruptedException {
        Instant start = Instant.now();
        Instant deadline = start.plus(Duration.ofMinutes(2));
        while (app.alive() && Instant.now().isBefore(deadline)) {
            try {
                // The metrics request itself is one active request. Active connections are not a
                // signal: the outbox relay holds one while it waits for the absent Kafka broker.
                if (app.metric("http.server.requests.active", "ACTIVE_TASKS") <= 1
                        && app.metric("hikaricp.connections.pending", "VALUE") == 0) {
                    break;
                }
            } catch (IOException | RuntimeException e) {
                // the server is still swamped or gone; try again until the deadline
            }
            Thread.sleep(250);
        }
        return Duration.between(start, Instant.now()).toMillis() / 1000.0;
    }

    private static boolean ledgerHolds(PostgreSQLContainer db) throws Exception {
        try {
            LedgerBenchmark.checkLedger(db);
            return true;
        } catch (AssertionError e) {
            System.out.println("  ledger check failed: " + e.getMessage());
            return false;
        }
    }

    private static Map<String, Object> row(Run run, int index, double rate, OpenLoad.Step step,
            LedgerBenchmark.ResourceSampler.Summary resources, double drainSeconds, boolean alive, boolean ledgerOk) {
        long cap = TIMEOUT.toNanos();
        int scheduled = step.scheduled();
        long created = step.count(s -> s == 201);
        long unavailable503 = step.count(s -> s == 503);
        long otherServerErrors = step.count(s -> s >= 500 && s != 503);
        long timeouts = step.count(s -> s == OpenLoad.TIMEOUT);
        long connectionErrors = step.count(s -> s == OpenLoad.CONNECTION_ERROR);
        long unsent = step.count(s -> s == OpenLoad.UNSENT);
        long errors = unavailable503 + otherServerErrors + timeouts + connectionErrors + unsent;
        double errorShare = scheduled == 0 ? 0 : (double) errors / scheduled;
        double p99 = OpenLoad.percentileMs(step.latencyNanos(), 99, cap);

        long[] createdLatency = new long[(int) created];
        int k = 0;
        for (int i = 0; i < scheduled; i++) {
            if (step.status()[i] == 201) {
                createdLatency[k++] = step.latencyNanos()[i];
            }
        }

        Map<String, Object> r = new LinkedHashMap<>();
        r.put("cell", run.cellId());
        r.put("strategy", STRATEGY);
        r.put("hotShare", run.hotShare());
        r.put("repetition", run.repetition());
        r.put("step", index);
        r.put("offeredRps", rate);
        r.put("seconds", round(step.seconds()));
        r.put("scheduled", scheduled);
        r.put("created", created);
        r.put("createdPerSecond", round(created / step.seconds()));
        r.put("answeredPerSecond", round(step.count(s -> s > 0) / step.seconds()));
        r.put("client4xx", step.count(s -> s >= 400 && s < 500));
        r.put("unavailable503", unavailable503);
        r.put("otherServerErrors", otherServerErrors);
        r.put("timeouts", timeouts);
        r.put("connectionErrors", connectionErrors);
        r.put("unsent", unsent);
        r.put("errorShare", round4(errorShare));
        // Every scheduled request, from its scheduled time; no answer counts as the timeout (ADR 0030).
        r.put("p50Ms", OpenLoad.percentileMs(step.latencyNanos(), 50, cap));
        r.put("p90Ms", OpenLoad.percentileMs(step.latencyNanos(), 90, cap));
        r.put("p99Ms", p99);
        r.put("p999Ms", OpenLoad.percentileMs(step.latencyNanos(), 99.9, cap));
        r.put("maxMs", OpenLoad.percentileMs(step.latencyNanos(), 100, cap));
        r.put("createdP99Ms", OpenLoad.percentileMs(createdLatency, 99, cap));
        r.put("sendLagP99Ms", OpenLoad.percentileMs(step.sendLagNanos(), 99, cap));
        r.put("sloPass", p99 <= SLO_P99_MS && errorShare <= SLO_ERROR_SHARE);
        r.put("ledgerOk", ledgerOk);
        r.put("appAlive", alive);
        r.put("peakPlatformThreads", Math.round(resources.peakPlatformThreads()));
        r.put("peakHeapMb", Math.round(resources.peakHeapMb()));
        r.put("meanAppCpu", round(resources.meanAppCpu()));
        r.put("meanLoadGeneratorCpu", round(resources.meanLoadGeneratorCpu()));
        r.put("drainSeconds", round(drainSeconds));
        r.put("finishedAt", Instant.now().toString());
        return r;
    }

    private static void writeRunInfo(int plannedRuns) throws IOException {
        Map<String, Object> info = new LinkedHashMap<>();
        info.put("startedAt", Instant.now().toString());
        info.put("githubRunId", System.getenv().getOrDefault("GITHUB_RUN_ID", "local"));
        info.put("cpu", LedgerBenchmark.cpuModel());
        info.put("logicalCpus", Runtime.getRuntime().availableProcessors());
        info.put("os", System.getProperty("os.name") + " " + System.getProperty("os.version"));
        info.put("java", Runtime.version().toString());
        info.put("strategy", STRATEGY);
        info.put("hotShares", HOT_SHARES.toString().replace(" ", ""));
        info.put("rates", RATES);
        info.put("ratesHot00", System.getProperty("ledger.bench.rates.hot00", ""));
        info.put("ratesHot90", System.getProperty("ledger.bench.rates.hot90", ""));
        info.put("step", STEP.toString());
        info.put("warmup", WARMUP.toString());
        info.put("warmupRate", WARMUP_RATE);
        info.put("timeout", TIMEOUT.toString());
        info.put("maxInFlight", MAX_IN_FLIGHT);
        info.put("stopAfterFailures", STOP_AFTER_FAILURES);
        info.put("repetitions", REPETITIONS);
        info.put("plannedRuns", plannedRuns);
        info.put("seed", SEED);
        info.put("sloP99Ms", SLO_P99_MS);
        info.put("sloErrorShare", SLO_ERROR_SHARE);
        Files.writeString(OUT.resolve("run.json"), LedgerBenchmark.json(info) + "\n", StandardCharsets.UTF_8);
    }

    private static List<Double> doubles(String csv) {
        return Arrays.stream(csv.split(",")).map(String::trim).filter(s -> !s.isEmpty()).map(Double::valueOf).toList();
    }

    private static void sleep(Duration duration) {
        try {
            Thread.sleep(duration);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static double round(double value) {
        return Math.round(value * 100) / 100.0;
    }

    private static double round4(double value) {
        return Math.round(value * 10_000) / 10_000.0;
    }
}
