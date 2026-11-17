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
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
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
import org.testcontainers.kafka.KafkaContainer;
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
 *
 * <p>Phase 7 and 8 cells ({@code ledger.bench.cells}, separated by {@code ;}) read
 * {@code name:hotShare:writeRates[:stepDurations[:readRates]]}, each part a comma-separated list
 * with one entry per step; a list of one entry applies to every step, and an empty duration list
 * means {@code ledger.bench.step}. A cell with read rates (phase 8, spec
 * {@code docs/specs/2026-11-18-pool-bounds}) runs two independent open-model schedules per step,
 * transfers at the write rate and {@code GET /accounts/{id}} of a random funded account at the
 * read rate, and reports latency, outcome and SLO per kind. Examples:
 *
 * <pre>
 * reads:0:800:PT2M:2000,4000,8000,16000,2000   five 2 minute steps, writes at 800, a read ladder
 * backlog:0:1100,400:PT10M,PT2M                10 minutes at 1,100 then 2 minutes at 400, no reads
 * </pre>
 *
 * <p>With {@code -Dledger.bench.kafka=true} (phase 8) every run also gets a Kafka container, so
 * the outbox relay publishes and the statement consumer runs, and the time series carries the
 * outbox and consumer series. The harness then waits after a cell's last step, and after every
 * step that had reads, until the outbox is empty and every transfer is in the statement (at most
 * 2 minutes), and records how long that took. Write-only steps of a cell run back to back, so a
 * backlog carries into the next step: that is what the {@code backlog} cell measures. Without the
 * flag nothing changes for the phase 6 and 7 runs.
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
    /** Repetition numbers start after this, so parallel jobs of one run write distinct ones. */
    private static final int REPETITION_OFFSET = Integer.getInteger("ledger.bench.repetition-offset", 0);
    /**
     * Phase 7 cells, {@code name:hotShare:rate,rate,...[:stepDuration]} separated by {@code ;},
     * each run once per arm (spec {@code docs/specs/2026-11-17-admission-control}). Empty: the phase 6 ladder per
     * hot share.
     */
    private static final String CELLS = System.getProperty("ledger.bench.cells", "");
    /** Phase 8: a Kafka broker next to PostgreSQL in every run. Off for the phase 6 and 7 runs. */
    private static final boolean KAFKA = Boolean.getBoolean("ledger.bench.kafka");
    private static final Duration SETTLE_CAP = Duration.ofMinutes(2);
    private static final List<String> ARMS =
            Arrays.stream(System.getProperty("ledger.bench.arms", "baseline").split(",")).map(String::trim).toList();
    /** Application arguments per arm, overridable with {@code ledger.bench.arm.<name>}. */
    private static final Map<String, String> ARM_ARGS = Map.of(
            // Phase 7: "baseline" is the limiter off, as the application was before ADR 0033.
            "baseline", AppProcess.ADMISSION_OFF,
            "hikari", AppProcess.ADMISSION_OFF + " --spring.datasource.hikari.connection-timeout=1000",
            "limiter", "--ledger.admission.max-concurrent=16 --ledger.admission.max-wait=PT0.1S",
            // Phase 8: "current" is the spec's baseline, the application as it is (write limiter on).
            "current", "",
            "pool1s", "--spring.datasource.hikari.connection-timeout=1000",
            "readlimit", "--ledger.admission.reads.max-concurrent=16 --ledger.admission.reads.max-wait=PT0.1S");

    /**
     * One cell and repetition. {@code name} is null for a phase 6 ladder, whose id stays as it
     * was so the phase 6 aggregator still reads it.
     */
    private record Run(String name, double hotShare, String arm, List<Double> rates, List<Duration> steps,
            List<Double> readRates, int repetition) {

        int stepCount() {
            return Math.max(rates.size(), Math.max(steps.size(), readRates.size()));
        }

        double rate(int index) {
            return rates.size() == 1 ? rates.getFirst() : rates.get(index);
        }

        Duration step(int index) {
            return steps.size() == 1 ? steps.getFirst() : steps.get(index);
        }

        double readRate(int index) {
            return readRates.isEmpty() ? 0 : readRates.size() == 1 ? readRates.getFirst() : readRates.get(index);
        }

        boolean hasReads() {
            return readRates.stream().anyMatch(r -> r > 0);
        }

        String cellId() {
            String base = "%s-hot%02d".formatted(STRATEGY, Math.round(hotShare * 100));
            return name == null ? base : "%s-hot%02d-%s".formatted(name, Math.round(hotShare * 100), arm);
        }

        List<String> appArgs() {
            String override = System.getProperty("ledger.bench.arm." + arm);
            if (override == null && !ARM_ARGS.containsKey(arm)) {
                throw new IllegalArgumentException("unknown arm " + arm);
            }
            String args = override != null ? override : ARM_ARGS.get(arm);
            return args.isBlank() ? List.of() : List.of(args.trim().split("\\s+"));
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
        for (int repetition = REPETITION_OFFSET + 1; repetition <= REPETITION_OFFSET + REPETITIONS; repetition++) {
            if (CELLS.isBlank()) {
                for (double hot : HOT_SHARES) {
                    List<Double> rates = doubles(System.getProperty(
                            "ledger.bench.rates.hot%02d".formatted(Math.round(hot * 100)), RATES));
                    runs.add(new Run(null, hot, "baseline", rates, List.of(STEP), List.of(), repetition));
                }
            } else {
                for (String spec : CELLS.split(";")) {
                    String[] parts = spec.trim().split(":", -1);
                    List<Duration> steps = parts.length > 3 && !parts[3].isBlank()
                            ? Arrays.stream(parts[3].split(",")).map(String::trim).map(Duration::parse).toList()
                            : List.of(STEP);
                    List<Double> reads = parts.length > 4 ? doubles(parts[4]) : List.of();
                    for (String arm : ARMS) {
                        Run run = new Run(parts[0], Double.parseDouble(parts[1]), arm, doubles(parts[2]), steps,
                                reads, repetition);
                        for (List<?> list : List.of(run.rates(), run.steps(), run.readRates())) {
                            if (list.size() > 1 && list.size() != run.stepCount()) {
                                throw new IllegalArgumentException("cell " + spec + ": lists of different lengths");
                            }
                        }
                        runs.add(run);
                    }
                }
            }
        }
        Collections.shuffle(runs, new Random(SEED + REPETITION_OFFSET));
        Files.createDirectories(OUT);
        writeRunInfo(runs.size());

        List<String> brokenLedgers = new ArrayList<>();
        int done = 0;
        for (Run run : runs) {
            System.out.printf("[%d/%d] %s rep %d%n", ++done, runs.size(), run.cellId(), run.repetition());
            if (CELLS.isBlank()) {
                brokenLedgers.addAll(climb(run));
                continue;
            }
            // Phase 7: one job is one block of the paired design; a run that cannot start must not
            // take the job's other cells down with it.
            try {
                brokenLedgers.addAll(climb(run));
            } catch (Exception | AssertionError e) {
                Map<String, Object> failure = new LinkedHashMap<>();
                failure.put("cell", run.cellId());
                failure.put("arm", run.arm());
                failure.put("repetition", run.repetition());
                failure.put("error", e.getClass().getSimpleName());
                Files.writeString(OUT.resolve("failures.jsonl"), LedgerBenchmark.json(failure) + "\n",
                        StandardCharsets.UTF_8, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
                brokenLedgers.add(run.cellId() + " rep " + run.repetition() + ": did not run, " + e);
            }
        }
        assertThat(brokenLedgers).as("steps with a failed ledger check or a 4xx answer, or runs that failed")
                .isEmpty();
    }

    /** One cell and repetition: warm-up, then the ladder. Returns the steps whose ledger check failed. */
    private List<String> climb(Run run) throws Exception {
        List<String> broken = new ArrayList<>();
        Cell cell = new Cell("breaking-point", STRATEGY, run.hotShare(), 0, true);
        // Testcontainers' default command, plus the PostgreSQL events that could stall a step
        // (spec 2026-11-17-admission-control, scope 1).
        try (PostgreSQLContainer db = new PostgreSQLContainer(DockerImageName.parse("postgres:18-alpine"))
                .withCommand("postgres", "-c", "fsync=off", "-c", "log_checkpoints=on",
                        "-c", "log_autovacuum_min_duration=0", "-c", "log_lock_waits=on");
                KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("apache/kafka:4.2.2"))) {
            db.start();
            List<String> appArgs = new ArrayList<>();
            if (KAFKA) {
                kafka.start();
                appArgs.addAll(AppProcess.kafkaArgs(kafka.getBootstrapServers()));
            }
            appArgs.addAll(run.appArgs()); // last, so an arm can override
            Path logs = OUT.resolve("logs");
            String prefix = run.cellId() + "-rep" + run.repetition();
            Path log = logs.resolve(prefix + ".log");
            Files.createDirectories(logs);
            List<String> gcLog = List.of("-Xlog:gc:file=\"" + logs.resolve(prefix + "-gc.log").toAbsolutePath()
                    + "\":utctime,uptime,level");
            try (AppProcess app = AppProcess.start(JAR, cell, db.getJdbcUrl(), db.getUsername(), db.getPassword(), log,
                    gcLog, appArgs);
                    HttpClient client = HttpClient.newBuilder()
                            .version(HttpClient.Version.HTTP_1_1)
                            .connectTimeout(TIMEOUT)
                            .build()) {
                List<String> accounts = LedgerBenchmark.seed(app);
                Supplier<HttpRequest.Builder> transfers = transfers(app, accounts, run.hotShare());
                Supplier<HttpRequest.Builder> reads = reads(app, accounts);

                // Discarded; reads are warmed too when the cell has them.
                OpenLoad.runMixed(client, transfers, WARMUP_RATE, reads, run.hasReads() ? WARMUP_RATE : 0, WARMUP,
                        TIMEOUT, MAX_IN_FLIGHT);
                requireDrainMetrics(app);
                drain(app);
                if (KAFKA) {
                    settle(db, app);
                }

                int failuresInARow = 0;
                for (int index = 0; index < run.stepCount(); index++) {
                    double rate = run.rate(index);
                    double readRate = run.readRate(index);
                    LedgerBenchmark.ResourceSampler sampler = LedgerBenchmark.ResourceSampler.start(app);
                    PoolSampler pool = PoolSampler.start(app);
                    OpenLoad.Mixed step = OpenLoad.runMixed(client, transfers, rate, reads, readRate, run.step(index),
                            TIMEOUT, MAX_IN_FLIGHT);
                    List<PoolSampler.Sample> poolSamples = pool.stop();
                    LedgerBenchmark.ResourceSampler.Summary resources = sampler.stop();
                    writeTimeSeries(run, index, rate, readRate, step, poolSamples);
                    Drain drain = drain(app);
                    boolean last = index == run.stepCount() - 1;
                    Drain settle = KAFKA && (last || readRate > 0) ? settle(db, app) : null;
                    boolean alive = app.alive();
                    boolean ledgerOk = ledgerHolds(db);
                    Map<String, Object> row = row(run, index, rate, readRate, step, resources, drain, settle, alive,
                            ledgerOk);
                    if (!alive) {
                        row.put("exit", app.tail().contains("OutOfMemoryError") ? "out-of-memory" : "exited");
                    }
                    Files.writeString(OUT.resolve("steps.jsonl"), LedgerBenchmark.json(row) + "\n", StandardCharsets.UTF_8,
                            StandardOpenOption.CREATE, StandardOpenOption.APPEND);
                    System.out.printf("  %5.0f req/s: %s created/s, p99 %s ms, errors %s, slo %s, ledger %s%n", rate,
                            row.get("createdPerSecond"), row.get("p99Ms"), row.get("errorShare"), row.get("sloPass"),
                            ledgerOk ? "ok" : "BROKEN");
                    if (readRate > 0) {
                        System.out.printf("  %5.0f reads/s: %s ok/s, p99 %s ms, errors %s, slo %s%n", readRate,
                                row.get("readOkPerSecond"), row.get("readP99Ms"), row.get("readErrorShare"),
                                row.get("readSloPass"));
                    }
                    if (settle != null) {
                        System.out.printf("  settled %s in %s s%n", settle.drained(), settle.seconds());
                    }
                    if (!ledgerOk) {
                        broken.add(run.cellId() + " rep " + run.repetition() + " at " + rate + " req/s: ledger check");
                    }
                    if ((long) row.get("client4xx") + (long) row.get("read4xx") > 0) {
                        // Every request is valid and every account funded, so a 4xx is a harness bug
                        // that would pass a fast wrong answer off as a fast answer.
                        broken.add(run.cellId() + " rep " + run.repetition() + " at " + rate + " req/s: "
                                + row.get("client4xx") + " write and " + row.get("read4xx") + " read client errors");
                    }
                    boolean pass = (boolean) row.get("sloPass") && (readRate <= 0 || (boolean) row.get("readSloPass"));
                    failuresInARow = pass ? 0 : failuresInARow + 1;
                    if (!alive || (STOP_AFTER_FAILURES > 0 && failuresInARow >= STOP_AFTER_FAILURES)) {
                        break;
                    }
                }
            } finally {
                Files.writeString(logs.resolve(prefix + "-postgres.log"), db.getLogs(), StandardCharsets.UTF_8);
            }
        }
        return broken;
    }

    /**
     * One JSON line per second of the step: outcomes of the requests scheduled in that second, per
     * kind, next to the pool, outbox and consumer sample taken closest to it. Read fields appear
     * only for a step with reads, outbox and consumer fields only with a broker.
     */
    private static void writeTimeSeries(Run run, int index, double rate, double readRate, OpenLoad.Mixed step,
            List<PoolSampler.Sample> samples) throws IOException {
        long cap = TIMEOUT.toNanos();
        int seconds = (int) Math.ceil(run.step(index).toMillis() / 1000.0);
        StringBuilder out = new StringBuilder();
        for (int second = 0; second < seconds; second++) {
            int[] writeRange = secondRange(step.writes(), rate, second);
            int[] readRange = secondRange(step.reads(), readRate, second);
            if (writeRange == null && readRange == null) {
                continue;
            }
            Map<String, Object> r = new LinkedHashMap<>();
            r.put("cell", run.cellId());
            r.put("arm", run.arm());
            r.put("repetition", run.repetition());
            r.put("step", index);
            r.put("offeredRps", rate);
            r.put("second", second);
            int[] status = writeRange == null ? new int[0]
                    : Arrays.copyOfRange(step.writes().status(), writeRange[0], writeRange[1]);
            long[] latency = writeRange == null ? new long[0]
                    : Arrays.copyOfRange(step.writes().latencyNanos(), writeRange[0], writeRange[1]);
            r.put("scheduled", status.length);
            r.put("created", Arrays.stream(status).filter(s -> s == 201).count());
            r.put("unavailable503", Arrays.stream(status).filter(s -> s == 503).count());
            r.put("otherServerErrors", Arrays.stream(status).filter(s -> s >= 500 && s != 503).count());
            r.put("timeouts", Arrays.stream(status).filter(s -> s == OpenLoad.TIMEOUT).count());
            r.put("p99Ms", OpenLoad.percentileMs(latency, 99, cap));
            if (readRate > 0) {
                int[] readStatus = readRange == null ? new int[0]
                        : Arrays.copyOfRange(step.reads().status(), readRange[0], readRange[1]);
                long[] readLatency = readRange == null ? new long[0]
                        : Arrays.copyOfRange(step.reads().latencyNanos(), readRange[0], readRange[1]);
                r.put("offeredReadRps", readRate);
                r.put("readScheduled", readStatus.length);
                r.put("readOk", Arrays.stream(readStatus).filter(s -> s == 200).count());
                r.put("read503", Arrays.stream(readStatus).filter(s -> s == 503).count());
                r.put("readOtherServerErrors", Arrays.stream(readStatus).filter(s -> s >= 500 && s != 503).count());
                r.put("readTimeouts", Arrays.stream(readStatus).filter(s -> s == OpenLoad.TIMEOUT).count());
                r.put("readP99Ms", OpenLoad.percentileMs(readLatency, 99, cap));
            }
            PoolSampler.Sample nearest = null;
            for (PoolSampler.Sample sample : samples) {
                if (nearest == null || Math.abs(sample.secondsIntoStep() - second)
                        < Math.abs(nearest.secondsIntoStep() - second)) {
                    nearest = sample;
                }
            }
            if (nearest != null && Math.abs(nearest.secondsIntoStep() - second) <= 1.5) {
                r.put("poolPending", nearest.pending());
                r.put("poolAcquireMeanMs", nearest.acquireMeanMs());
                r.put("requestsActive", nearest.active());
                if (KAFKA) {
                    r.put("outboxPending", nearest.outboxPending());
                    r.put("outboxOldestAgeSeconds", nearest.outboxOldestAgeSeconds());
                    r.put("consumerLag", nearest.consumerLag());
                }
            }
            out.append(LedgerBenchmark.json(r)).append('\n');
        }
        Files.writeString(OUT.resolve("timeseries.jsonl"), out.toString(), StandardCharsets.UTF_8,
                StandardOpenOption.CREATE, StandardOpenOption.APPEND);
    }

    /** Indexes {@code [from, to)} of the requests scheduled in that second, or null when there are none. */
    private static int[] secondRange(OpenLoad.Step step, double rate, int second) {
        if (rate <= 0) {
            return null;
        }
        int from = (int) Math.ceil(second * rate);
        int to = (int) Math.min(step.scheduled(), Math.ceil((second + 1) * rate));
        return from < to ? new int[] {from, to} : null;
    }

    /** {@code GET /accounts/{id}} of a random funded account. */
    private static Supplier<HttpRequest.Builder> reads(AppProcess app, List<String> ids) {
        return () -> HttpRequest.newBuilder(
                URI.create(app.baseUrl + "/accounts/" + ids.get(ThreadLocalRandom.current().nextInt(ids.size())))).GET();
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
    private static Drain drain(AppProcess app) throws InterruptedException {
        Instant start = Instant.now();
        Instant deadline = start.plus(Duration.ofMinutes(2));
        boolean drained = false;
        while (app.alive() && Instant.now().isBefore(deadline)) {
            try {
                // The metrics request itself is one active request. Active connections are not a
                // signal: the outbox relay holds one while it waits for the absent Kafka broker.
                if (app.metric("http.server.requests.active", "ACTIVE_TASKS") <= 1
                        && app.metric("hikaricp.connections.pending", "VALUE") == 0) {
                    drained = true;
                    break;
                }
            } catch (IOException | RuntimeException e) {
                // the server is still swamped or gone; try again until the deadline
            }
            Thread.sleep(250);
        }
        return new Drain(drained, Duration.between(start, Instant.now()).toMillis() / 1000.0);
    }

    private record Drain(boolean drained, double seconds) {
    }

    /**
     * Phase 8: waits until no outbox row is pending and every transfer is in the statement (the
     * reconciliation's "missing from statement" is empty), read straight from the database.
     * Returns the seconds it took; gives up after 2 minutes or when the application is gone.
     */
    private static Drain settle(PostgreSQLContainer db, AppProcess app) throws InterruptedException {
        Instant start = Instant.now();
        Instant deadline = start.plus(SETTLE_CAP);
        boolean settled = false;
        try (Connection c = DriverManager.getConnection(db.getJdbcUrl(), db.getUsername(), db.getPassword());
                Statement s = c.createStatement()) {
            while (app.alive() && Instant.now().isBefore(deadline)) {
                try (ResultSet rs = s.executeQuery("""
                        select (select count(*) from outbox_events where published_at is null and parked_at is null)
                             + (select count(*) from transfers t where not exists
                                   (select 1 from statement.statement_lines l where l.transfer_id = t.id))
                        """)) {
                    rs.next();
                    if (rs.getLong(1) == 0) {
                        settled = true;
                        break;
                    }
                }
                Thread.sleep(500);
            }
        } catch (SQLException e) {
            System.out.println("  settle check failed: " + e.getMessage());
        }
        return new Drain(settled, Duration.between(start, Instant.now()).toMillis() / 1000.0);
    }

    /**
     * {@link AppProcess#metric} reads a missing metric as 0, which would make {@link #drain}
     * return at once; fail the run instead if a drain metric is not there.
     */
    private static void requireDrainMetrics(AppProcess app) throws IOException, InterruptedException {
        for (String name : List.of("http.server.requests.active", "hikaricp.connections.pending")) {
            assertThat(app.get("/actuator/metrics/" + name).statusCode()).as("metric " + name).isEqualTo(200);
        }
    }

    private static boolean ledgerHolds(PostgreSQLContainer db) {
        try {
            LedgerBenchmark.checkLedger(db);
            return true;
        } catch (AssertionError | SQLException e) {
            System.out.println("  ledger check failed: " + e.getMessage());
            return false;
        }
    }

    private static Map<String, Object> row(Run run, int index, double rate, double readRate, OpenLoad.Mixed mixed,
            LedgerBenchmark.ResourceSampler.Summary resources, Drain drain, Drain settle, boolean alive,
            boolean ledgerOk) {
        long cap = TIMEOUT.toNanos();
        OpenLoad.Step step = mixed.writes();
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
        r.put("name", run.name() == null ? "ladder" : run.name());
        r.put("arm", run.arm());
        r.put("strategy", STRATEGY);
        r.put("hotShare", run.hotShare());
        r.put("repetition", run.repetition());
        r.put("step", index);
        r.put("offeredRps", rate);
        r.put("stepSeconds", run.step(index).toSeconds());
        r.put("seconds", round(step.seconds()));
        r.put("scheduled", scheduled);
        r.put("created", created);
        // Per nominal step length: the wall time also holds the wait for the last timeouts.
        double nominal = run.step(index).toMillis() / 1000.0;
        r.put("createdPerSecond", round(created / nominal));
        r.put("answeredPerSecond", round(step.count(s -> s > 0) / nominal));
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
        // A step the server could not drain within 2 minutes, or did not survive, is not within
        // the SLO whatever its percentiles say.
        r.put("sloPass", p99 <= SLO_P99_MS && errorShare <= SLO_ERROR_SHARE && drain.drained() && alive);
        // Phase 8: the read schedule of the same step, judged by the same SLO on its own.
        OpenLoad.Step reads = mixed.reads();
        long readErrors = reads.count(s -> s >= 500 || s < 0);
        double readErrorShare = reads.scheduled() == 0 ? 0 : (double) readErrors / reads.scheduled();
        double readP99 = OpenLoad.percentileMs(reads.latencyNanos(), 99, cap);
        r.put("offeredReadRps", readRate);
        r.put("read4xx", reads.count(s -> s >= 400 && s < 500));
        if (readRate > 0) {
            r.put("readScheduled", reads.scheduled());
            r.put("readOk", reads.count(s -> s == 200));
            r.put("readOkPerSecond", round(reads.count(s -> s == 200) / nominal));
            r.put("read503", reads.count(s -> s == 503));
            r.put("readOtherServerErrors", reads.count(s -> s >= 500 && s != 503));
            r.put("readTimeouts", reads.count(s -> s == OpenLoad.TIMEOUT));
            r.put("readConnectionErrors", reads.count(s -> s == OpenLoad.CONNECTION_ERROR));
            r.put("readUnsent", reads.count(s -> s == OpenLoad.UNSENT));
            r.put("readErrorShare", round4(readErrorShare));
            r.put("readP50Ms", OpenLoad.percentileMs(reads.latencyNanos(), 50, cap));
            r.put("readP99Ms", readP99);
            r.put("readMaxMs", OpenLoad.percentileMs(reads.latencyNanos(), 100, cap));
            r.put("readSloPass", readP99 <= SLO_P99_MS && readErrorShare <= SLO_ERROR_SHARE && drain.drained()
                    && alive);
        }
        r.put("ledgerOk", ledgerOk);
        r.put("appAlive", alive);
        r.put("peakPlatformThreads", Math.round(resources.peakPlatformThreads()));
        r.put("peakHeapMb", Math.round(resources.peakHeapMb()));
        r.put("meanAppCpu", round(resources.meanAppCpu()));
        r.put("meanLoadGeneratorCpu", round(resources.meanLoadGeneratorCpu()));
        r.put("drained", drain.drained());
        r.put("drainSeconds", round(drain.seconds()));
        // Phase 8: outbox empty and every transfer in the statement; null when not measured.
        r.put("settled", settle == null ? null : settle.drained());
        r.put("settleSeconds", settle == null ? null : round(settle.seconds()));
        r.put("startedAt", step.startedAt().toString());
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
        for (double hot : HOT_SHARES) { // the ladder each cell actually climbs
            String key = "hot%02d".formatted(Math.round(hot * 100));
            info.put("rates." + key, System.getProperty("ledger.bench.rates." + key, RATES).replace(" ", ""));
        }
        info.put("step", STEP.toString());
        info.put("warmup", WARMUP.toString());
        info.put("warmupRate", WARMUP_RATE);
        info.put("timeout", TIMEOUT.toString());
        info.put("maxInFlight", MAX_IN_FLIGHT);
        info.put("stopAfterFailures", STOP_AFTER_FAILURES);
        info.put("repetitions", REPETITIONS);
        info.put("repetitionOffset", REPETITION_OFFSET);
        info.put("cells", CELLS);
        info.put("arms", String.join(",", ARMS));
        info.put("kafka", KAFKA);
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
