package tech.lucassantana.ledger.benchmark;

import static org.assertj.core.api.Assertions.assertThat;
import static us.abstracta.jmeter.javadsl.JmeterDsl.httpSampler;
import static us.abstracta.jmeter.javadsl.JmeterDsl.jsr223PreProcessor;
import static us.abstracta.jmeter.javadsl.JmeterDsl.testPlan;
import static us.abstracta.jmeter.javadsl.JmeterDsl.threadGroup;

import com.sun.management.OperatingSystemMXBean;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.http.HttpResponse;
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
import java.util.DoubleSummaryStatistics;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadLocalRandom;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.http.entity.ContentType;
import org.junit.jupiter.api.Test;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;
import us.abstracta.jmeter.javadsl.core.TestPlanStats;
import us.abstracta.jmeter.javadsl.core.stats.StatsSummary;

/**
 * The phase 2 benchmark (spec {@code docs/specs/2026-11-13-phase-2-benchmarks}, ADRs 0010 to 0014).
 *
 * <p>For each cell, in shuffled order: a fresh PostgreSQL, the application as a child JVM with
 * the cell's settings, 1,000 funded accounts, a warm-up whose numbers are discarded, a measured
 * window, then a ledger check. One JSON line per cell goes to {@code target/benchmark/results.jsonl}.
 *
 * <pre>
 * ./mvnw -Pbenchmark verify                                  # everything: 24 cells, 1 repetition
 * ./mvnw -Pbenchmark verify -Dledger.bench.repetitions=5     # the published run (ADR 0013)
 * ./mvnw -Pbenchmark verify -Dledger.bench.cells=strategy-pessimistic-hot30 \
 *        -Dledger.bench.warmup=PT5S -Dledger.bench.measure=PT10S   # a quick smoke run
 * </pre>
 */
class LedgerBenchmark {

    private static final int ACCOUNTS = 1_000;
    /** 1,000,000.00 each: with transfers of at most 1.00, no account runs dry during a cell. */
    private static final String INITIAL_BALANCE = "1000000.00";
    private static final Pattern ID = Pattern.compile("\"id\"\\s*:\\s*\"([0-9a-f-]{36})\"");

    private static final String GRID = System.getProperty("ledger.bench.grid", "all");
    private static final String CELL_FILTER = System.getProperty("ledger.bench.cells", "");
    private static final int REPETITIONS = Integer.getInteger("ledger.bench.repetitions", 1);
    private static final Duration WARMUP = Duration.parse(System.getProperty("ledger.bench.warmup", "PT30S"));
    private static final Duration MEASURE = Duration.parse(System.getProperty("ledger.bench.measure", "PT60S"));
    private static final long SEED = Long.getLong("ledger.bench.seed", 20261113L);
    private static final Path JAR = Path.of(System.getProperty("ledger.bench.jar", "target/double-entry-ledger-0.0.1-SNAPSHOT.jar"));
    private static final Path OUT = Path.of(System.getProperty("ledger.bench.out", "target/benchmark"));

    private record Run(Cell cell, int repetition) {
    }

    @Test
    void runCells() throws Exception {
        assertThat(JAR).as("packaged application").exists();
        List<Run> runs = plannedRuns();
        assertThat(runs).as("cells matching grid=%s cells=%s", GRID, CELL_FILTER).isNotEmpty();
        Files.createDirectories(OUT);
        writeRunInfo(runs.size());

        int done = 0;
        for (Run run : runs) {
            Map<String, Object> result = measure(run);
            Files.writeString(OUT.resolve("results.jsonl"), json(result) + "\n", StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE, StandardOpenOption.APPEND);
            System.out.printf("[%d/%d] %s rep %d: %.0f created/s, p99 %s ms, %s retries, %s x 503%n",
                    ++done, runs.size(), run.cell().id(), run.repetition(), result.get("createdPerSecond"),
                    result.get("p99Ms"), result.get("retries"), result.get("unavailable503"));
        }
    }

    /** Every selected cell times every repetition, shuffled so slow periods of the machine spread out (ADR 0013). */
    private static List<Run> plannedRuns() {
        List<Cell> cells = new ArrayList<>();
        if (GRID.equals("all") || GRID.equals("strategy")) {
            cells.addAll(Cell.strategyGrid());
        }
        if (GRID.equals("all") || GRID.equals("threads")) {
            cells.addAll(Cell.threadGrid());
        }
        List<Run> runs = new ArrayList<>();
        for (int repetition = 1; repetition <= REPETITIONS; repetition++) {
            for (Cell cell : cells) {
                if (matchesFilter(cell)) {
                    runs.add(new Run(cell, repetition));
                }
            }
        }
        Collections.shuffle(runs, new Random(SEED));
        return runs;
    }

    /** {@code ledger.bench.cells}: comma-separated substrings of cell ids; empty selects every cell. */
    private static boolean matchesFilter(Cell cell) {
        return CELL_FILTER.isBlank()
                || Arrays.stream(CELL_FILTER.split(",")).map(String::trim).anyMatch(cell.id()::contains);
    }

    private Map<String, Object> measure(Run run) throws Exception {
        Cell cell = run.cell();
        try (PostgreSQLContainer db = new PostgreSQLContainer(DockerImageName.parse("postgres:18-alpine"))) {
            db.start();
            Path log = OUT.resolve("logs").resolve(cell.id() + "-rep" + run.repetition() + ".log");
            try (AppProcess app = AppProcess.start(JAR, cell, db.getJdbcUrl(), db.getUsername(), db.getPassword(), log)) {
                List<String> accounts = seed(app);

                load(app, accounts, cell, WARMUP); // numbers discarded

                Snapshot before = Snapshot.take(app, db);
                ResourceSampler sampler = ResourceSampler.start(app);
                Instant start = Instant.now();
                TestPlanStats stats = load(app, accounts, cell, MEASURE);
                double seconds = Duration.between(start, Instant.now()).toMillis() / 1000.0;
                ResourceSampler.Summary resources = sampler.stop();
                Snapshot after = Snapshot.take(app, db);

                checkLedger(db);
                return result(run, stats, seconds, before, after, resources);
            }
        }
    }

    /**
     * Opens the accounts and funds them through the API, so they hold the ledger's invariants
     * from the start. Accounts open in parallel; deposits run one at a time, because every
     * deposit debits the settlement account (ADR 0008), and in parallel they make it the hottest
     * row of all, which under the optimistic and serializable strategies ends in 503s.
     */
    private static List<String> seed(AppProcess app) throws Exception {
        List<String> ids = Collections.synchronizedList(new ArrayList<>());
        Semaphore inFlight = new Semaphore(32);
        List<Future<?>> pending = new ArrayList<>();
        try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
            for (int i = 0; i < ACCOUNTS; i++) {
                inFlight.acquire();
                pending.add(executor.submit(() -> {
                    try {
                        HttpResponse<String> opened = app.post("/accounts", null, null);
                        Matcher id = ID.matcher(opened.body());
                        if (opened.statusCode() != 201 || !id.find()) {
                            throw new IllegalStateException("open account: " + opened.statusCode() + " " + opened.body());
                        }
                        ids.add(id.group(1));
                        return null;
                    } finally {
                        inFlight.release();
                    }
                }));
            }
        }
        for (Future<?> f : pending) {
            f.get();
        }
        List<String> sorted = new ArrayList<>(ids);
        Collections.sort(sorted); // the hot account is ids.getFirst(), the same rule in every cell
        for (String id : sorted) {
            deposit(app, id);
        }
        return List.copyOf(sorted);
    }

    /** A well-behaved client: 409 and 503 mean nothing was written yet, so retry with the same key. */
    private static void deposit(AppProcess app, String accountId) throws IOException, InterruptedException {
        String key = UUID.randomUUID().toString();
        String body = "{\"accountId\":\"%s\",\"amount\":\"%s\"}".formatted(accountId, INITIAL_BALANCE);
        for (int attempt = 0; attempt < 100; attempt++) {
            HttpResponse<String> deposit = app.post("/deposits", key, body);
            if (deposit.statusCode() == 201) {
                return;
            }
            if (deposit.statusCode() != 409 && deposit.statusCode() != 503) {
                throw new IllegalStateException("deposit: " + deposit.statusCode() + " " + deposit.body());
            }
            Thread.sleep(10);
        }
        throw new IllegalStateException("deposit to " + accountId + " kept conflicting for 100 attempts");
    }

    /**
     * Closed model (ADR 0012): {@code users} JMeter threads, each sending a transfer as soon as
     * the previous one answered, for {@code duration}. With probability {@code hotShare} the
     * transfer has the hot account on one side (direction at random); otherwise both sides are
     * uniform over all accounts. Amounts from 0.01 to 1.00, every request with a fresh key.
     */
    private static TestPlanStats load(AppProcess app, List<String> ids, Cell cell, Duration duration)
            throws IOException, InterruptedException {
        String hot = ids.getFirst();
        int n = ids.size();
        double hotShare = cell.hotShare();
        return testPlan(
                threadGroup(cell.users(), duration,
                        httpSampler(app.baseUrl + "/transfers")
                                .post("{\"fromAccountId\":\"${from}\",\"toAccountId\":\"${to}\",\"amount\":\"${amount}\"}",
                                        ContentType.APPLICATION_JSON)
                                .header("Idempotency-Key", "${__UUID()}")
                                .children(jsr223PreProcessor(s -> {
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
                                    s.vars.put("from", from);
                                    s.vars.put("to", to);
                                    s.vars.put("amount", "%d.%02d".formatted(cents / 100, cents % 100));
                                }))))
                .run();
    }

    /** Counters read before and after the measured window; the result is their difference. */
    private record Snapshot(double created, double conflict409, double unavailable503, double retries, long deadlocks) {

        static Snapshot take(AppProcess app, PostgreSQLContainer db) throws Exception {
            return new Snapshot(
                    app.metric("http.server.requests", "COUNT", "uri:/transfers", "status:201"),
                    app.metric("http.server.requests", "COUNT", "uri:/transfers", "status:409"),
                    app.metric("http.server.requests", "COUNT", "uri:/transfers", "status:503"),
                    app.metric("ledger.transfer.conflict.retries", "COUNT"),
                    queryLong(db, "select deadlocks from pg_stat_database where datname = current_database()"));
        }
    }

    /**
     * Polls the application's JVM gauges once a second during the measured window: peak live
     * platform threads (virtual threads are not counted by the JVM's thread gauge), peak used heap
     * and mean process CPU; and the load generator's own CPU, reported next to it (ADR 0011).
     */
    private static final class ResourceSampler {

        record Summary(double peakPlatformThreads, double peakHeapMb, double meanAppCpu, double meanLoadGeneratorCpu) {
        }

        private final AppProcess app;
        private final DoubleSummaryStatistics threads = new DoubleSummaryStatistics();
        private final DoubleSummaryStatistics heap = new DoubleSummaryStatistics();
        private final DoubleSummaryStatistics cpu = new DoubleSummaryStatistics();
        private final DoubleSummaryStatistics generatorCpu = new DoubleSummaryStatistics();
        private final OperatingSystemMXBean os = (OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
        private volatile boolean running = true;
        private Thread thread;

        private ResourceSampler(AppProcess app) {
            this.app = app;
        }

        static ResourceSampler start(AppProcess app) {
            ResourceSampler sampler = new ResourceSampler(app);
            sampler.thread = Thread.ofVirtual().start(sampler::loop);
            return sampler;
        }

        private void loop() {
            while (running) {
                try {
                    threads.accept(app.metric("jvm.threads.live", "VALUE"));
                    heap.accept(app.metric("jvm.memory.used", "VALUE", "area:heap") / (1024 * 1024));
                    cpu.accept(app.metric("process.cpu.usage", "VALUE"));
                    generatorCpu.accept(os.getProcessCpuLoad());
                    Thread.sleep(1_000);
                } catch (InterruptedException e) {
                    return;
                } catch (IOException e) {
                    // A missed sample under load is acceptable; the next one comes in a second.
                }
            }
        }

        Summary stop() throws InterruptedException {
            running = false;
            thread.interrupt();
            thread.join();
            return new Summary(threads.getMax(), heap.getMax(), cpu.getAverage(), generatorCpu.getAverage());
        }
    }

    /** Invariants 1 to 3 of the phase 1 spec: a benchmark never reports numbers from a broken ledger (ADR 0014). */
    private static void checkLedger(PostgreSQLContainer db) throws SQLException {
        assertThat(queryLong(db, "select coalesce(sum(balance_cents), 0) from accounts")).as("sum of balances").isZero();
        assertThat(queryLong(db, "select count(*) from accounts where kind = 'CUSTOMER' and balance_cents < 0"))
                .as("negative customer balances").isZero();
        assertThat(queryLong(db, """
                select count(*) from accounts a
                where a.balance_cents <> coalesce((select sum(e.amount_cents) from entries e where e.account_id = a.id), 0)
                """)).as("balances that differ from their entries").isZero();
    }

    private static long queryLong(PostgreSQLContainer db, String sql) throws SQLException {
        try (Connection c = DriverManager.getConnection(db.getJdbcUrl(), db.getUsername(), db.getPassword());
                Statement s = c.createStatement();
                ResultSet rs = s.executeQuery(sql)) {
            rs.next();
            return rs.getLong(1);
        }
    }

    private static Map<String, Object> result(Run run, TestPlanStats stats, double seconds, Snapshot before,
            Snapshot after, ResourceSampler.Summary resources) {
        Cell cell = run.cell();
        StatsSummary overall = stats.overall();
        Map<String, Object> r = new LinkedHashMap<>();
        r.put("cell", cell.id());
        r.put("grid", cell.grid());
        r.put("strategy", cell.strategy());
        r.put("hotShare", cell.hotShare());
        r.put("users", cell.users());
        r.put("threads", cell.virtualThreads() ? "virtual" : "platform");
        r.put("repetition", run.repetition());
        r.put("measuredSeconds", round(seconds));
        r.put("requests", overall.samplesCount());
        r.put("errors", overall.errorsCount());
        r.put("createdPerSecond", round((after.created() - before.created()) / seconds));
        r.put("p50Ms", overall.sampleTime().median().toMillis());
        r.put("p95Ms", overall.sampleTime().perc95().toMillis());
        r.put("p99Ms", overall.sampleTime().perc99().toMillis());
        r.put("maxMs", overall.sampleTime().max().toMillis());
        r.put("retries", Math.round(after.retries() - before.retries()));
        r.put("unavailable503", Math.round(after.unavailable503() - before.unavailable503()));
        r.put("conflict409", Math.round(after.conflict409() - before.conflict409()));
        r.put("deadlocks", after.deadlocks() - before.deadlocks());
        r.put("peakPlatformThreads", Math.round(resources.peakPlatformThreads()));
        r.put("peakHeapMb", Math.round(resources.peakHeapMb()));
        r.put("meanAppCpu", round(resources.meanAppCpu()));
        r.put("meanLoadGeneratorCpu", round(resources.meanLoadGeneratorCpu()));
        r.put("finishedAt", Instant.now().toString());
        return r;
    }

    /** Machine and settings of this run, so results are only ever compared within one run (ADR 0013). */
    private static void writeRunInfo(int plannedCells) throws IOException {
        Map<String, Object> info = new LinkedHashMap<>();
        info.put("startedAt", Instant.now().toString());
        info.put("githubRunId", System.getenv().getOrDefault("GITHUB_RUN_ID", "local"));
        info.put("cpu", cpuModel());
        info.put("logicalCpus", Runtime.getRuntime().availableProcessors());
        info.put("os", System.getProperty("os.name") + " " + System.getProperty("os.version"));
        info.put("java", Runtime.version().toString());
        info.put("grid", GRID);
        info.put("cellFilter", CELL_FILTER);
        info.put("repetitions", REPETITIONS);
        info.put("plannedCells", plannedCells);
        info.put("warmup", WARMUP.toString());
        info.put("measure", MEASURE.toString());
        info.put("seed", SEED);
        Files.writeString(OUT.resolve("run.json"), json(info) + "\n", StandardCharsets.UTF_8);
    }

    private static String cpuModel() {
        Path cpuinfo = Path.of("/proc/cpuinfo");
        try {
            if (Files.exists(cpuinfo)) {
                for (String line : Files.readAllLines(cpuinfo)) {
                    if (line.startsWith("model name")) {
                        return line.substring(line.indexOf(':') + 1).trim();
                    }
                }
            }
        } catch (IOException e) {
            // fall through
        }
        return System.getenv().getOrDefault("PROCESSOR_IDENTIFIER", "unknown");
    }

    private static double round(double value) {
        return Math.round(value * 100) / 100.0;
    }

    /** Flat JSON object; values are numbers or strings without quotes or backslashes. */
    private static String json(Map<String, Object> fields) {
        StringBuilder out = new StringBuilder("{");
        fields.forEach((key, value) -> {
            if (out.length() > 1) {
                out.append(',');
            }
            out.append('"').append(key).append("\":");
            out.append(value instanceof Number ? value.toString() : "\"" + value + "\"");
        });
        return out.append('}').toString();
    }
}
