package tech.lucassantana.ledger.benchmark;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * The application under test, started as a child JVM from the packaged jar (ADR 0014), so its
 * threads, heap and CPU are its own and not the load generator's.
 */
final class AppProcess implements AutoCloseable {

    private static final Pattern MEASUREMENT =
            Pattern.compile("\"statistic\"\\s*:\\s*\"(\\w+)\"\\s*,\\s*\"value\"\\s*:\\s*([-0-9.Ee+]+)");

    private final Process process;
    private final Path log;
    private final HttpClient http = HttpClient.newHttpClient();
    final String baseUrl;

    private AppProcess(Process process, Path log, int port) {
        this.process = process;
        this.log = log;
        this.baseUrl = "http://localhost:" + port;
    }

    /** Starts the jar with the cell's settings and waits until it reports healthy. */
    static AppProcess start(Path jar, Cell cell, String jdbcUrl, String user, String password, Path log)
            throws IOException, InterruptedException {
        int port = freePort();
        String java = ProcessHandle.current().info().command().orElse("java");
        List<String> command = new ArrayList<>(List.of(
                java,
                // Pinned so every cell runs the same JVM size; the heap metric is read against it.
                "-Xms1g", "-Xmx1g",
                "-jar", jar.toString(),
                "--server.port=" + port,
                "--spring.datasource.url=" + jdbcUrl,
                "--spring.datasource.username=" + user,
                "--spring.datasource.password=" + password,
                "--ledger.concurrency.strategy=" + cell.strategy(),
                "--spring.threads.virtual.enabled=" + cell.virtualThreads(),
                // The defaults, pinned: the thread grid's hypothesis rests on these two numbers (ADR 0012).
                "--spring.datasource.hikari.maximum-pool-size=10",
                "--server.tomcat.threads.max=200",
                "--management.endpoints.web.exposure.include=health,metrics",
                // Conflict aborts are counted by the retry counters; logging each one would add
                // I/O to exactly the strategies that abort, skewing the comparison.
                "--logging.level.org.hibernate.orm.jdbc.error=ERROR"));
        Files.createDirectories(log.getParent());
        Process process = new ProcessBuilder(command)
                .redirectErrorStream(true)
                .redirectOutput(log.toFile())
                .start();
        AppProcess app = new AppProcess(process, log, port);
        try {
            app.awaitHealthy(Duration.ofSeconds(120));
        } catch (RuntimeException | IOException | InterruptedException e) {
            app.close();
            throw e;
        }
        return app;
    }

    private void awaitHealthy(Duration timeout) throws InterruptedException, IOException {
        Instant deadline = Instant.now().plus(timeout);
        while (Instant.now().isBefore(deadline)) {
            if (!process.isAlive()) {
                throw new IllegalStateException("application exited with " + process.exitValue() + ":\n" + tail());
            }
            try {
                if (get("/actuator/health").statusCode() == 200) {
                    return;
                }
            } catch (IOException notListeningYet) {
                // keep polling
            }
            Thread.sleep(250);
        }
        throw new IllegalStateException("application not healthy after " + timeout + ":\n" + tail());
    }

    HttpResponse<String> get(String path) throws IOException, InterruptedException {
        return http.send(HttpRequest.newBuilder(URI.create(baseUrl + path)).GET().build(),
                HttpResponse.BodyHandlers.ofString());
    }

    HttpResponse<String> post(String path, String idempotencyKey, String json) throws IOException, InterruptedException {
        HttpRequest.Builder request = HttpRequest.newBuilder(URI.create(baseUrl + path))
                .header("Content-Type", "application/json")
                .POST(json == null ? HttpRequest.BodyPublishers.noBody() : HttpRequest.BodyPublishers.ofString(json));
        if (idempotencyKey != null) {
            request.header("Idempotency-Key", idempotencyKey);
        }
        return http.send(request.build(), HttpResponse.BodyHandlers.ofString());
    }

    /**
     * One statistic of an Actuator metric, for example {@code COUNT} or {@code VALUE}. Returns 0
     * when the metric has no data yet (Actuator answers 404 for a tag value never seen).
     *
     * @param tags {@code key:value} pairs
     */
    double metric(String name, String statistic, String... tags) throws IOException, InterruptedException {
        StringBuilder path = new StringBuilder("/actuator/metrics/").append(name);
        for (int i = 0; i < tags.length; i++) {
            path.append(i == 0 ? '?' : '&').append("tag=").append(tags[i]);
        }
        HttpResponse<String> response = get(path.toString());
        if (response.statusCode() == 404) {
            return 0;
        }
        if (response.statusCode() != 200) {
            throw new IllegalStateException(path + " answered " + response.statusCode());
        }
        Matcher m = MEASUREMENT.matcher(response.body());
        while (m.find()) {
            if (m.group(1).equals(statistic)) {
                return Double.parseDouble(m.group(2));
            }
        }
        throw new IllegalStateException(path + " has no " + statistic + ": " + response.body());
    }

    private String tail() throws IOException {
        List<String> lines = Files.readAllLines(log);
        return String.join("\n", lines.subList(Math.max(0, lines.size() - 40), lines.size()));
    }

    private static int freePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    @Override
    public void close() throws InterruptedException {
        process.destroy();
        if (!process.waitFor(30, TimeUnit.SECONDS)) {
            process.destroyForcibly().waitFor(30, TimeUnit.SECONDS);
        }
    }
}
