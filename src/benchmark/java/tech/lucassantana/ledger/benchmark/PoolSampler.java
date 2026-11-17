package tech.lucassantana.ledger.benchmark;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Samples the application's database pool and request queue once a second during a step (spec
 * {@code docs/specs/2026-11-17-admission-control}, scope 1): threads waiting for a connection,
 * the mean connection acquire time since the last answered read, and requests in progress. The
 * three reads go out together and the sample is stamped when they finished; a read the swamped
 * server does not answer within a second is recorded as missing (null).
 */
final class PoolSampler {

    /** One second of a step; a field is null when the application did not answer in time. */
    record Sample(double secondsIntoStep, Double pending, Double acquireMeanMs, Double active) {
    }

    private static final Duration POLL_TIMEOUT = Duration.ofSeconds(1);

    private final AppProcess app;
    private final List<Sample> samples = new ArrayList<>();
    private final Thread thread;
    private final long startNanos = System.nanoTime();
    private volatile boolean running = true;

    private PoolSampler(AppProcess app) {
        this.app = app;
        this.thread = Thread.ofVirtual().start(this::loop);
    }

    static PoolSampler start(AppProcess app) {
        return new PoolSampler(app);
    }

    List<Sample> stop() throws InterruptedException {
        running = false;
        thread.join();
        return List.copyOf(samples);
    }

    private void loop() {
        double lastCount = Double.NaN;
        double lastTotal = Double.NaN;
        long next = startNanos;
        while (running && app.alive()) {
            next += 1_000_000_000L;
            var pendingRead = java.util.concurrent.CompletableFuture.supplyAsync(
                    () -> value("hikaricp.connections.pending", "VALUE"), Thread::startVirtualThread);
            var activeRead = java.util.concurrent.CompletableFuture.supplyAsync(
                    () -> value("http.server.requests.active", "ACTIVE_TASKS"), Thread::startVirtualThread);
            var acquireRead = java.util.concurrent.CompletableFuture.supplyAsync(
                    () -> read("hikaricp.connections.acquire"), Thread::startVirtualThread);
            Double pending = pendingRead.join();
            Double active = activeRead.join();
            Map<String, Double> acquire = acquireRead.join();
            double at = (System.nanoTime() - startNanos) / 1e9;
            Double acquireMean = null;
            if (acquire != null && acquire.containsKey("COUNT") && acquire.containsKey("TOTAL_TIME")) {
                double count = acquire.get("COUNT");
                double total = acquire.get("TOTAL_TIME"); // seconds
                if (!Double.isNaN(lastCount) && count > lastCount) {
                    acquireMean = Math.round((total - lastTotal) / (count - lastCount) * 1e5) / 100.0;
                } else if (!Double.isNaN(lastCount)) {
                    acquireMean = 0.0;
                }
                lastCount = count;
                lastTotal = total;
            }
            samples.add(new Sample(Math.round(at * 10) / 10.0, pending, acquireMean, active));
            long sleep = next - System.nanoTime();
            if (sleep > 0) {
                try {
                    Thread.sleep(Duration.ofNanos(sleep));
                } catch (InterruptedException e) {
                    return;
                }
            }
        }
    }

    private Double value(String name, String statistic) {
        Map<String, Double> values = read(name);
        return values == null ? null : values.get(statistic);
    }

    private Map<String, Double> read(String name) {
        try {
            return app.measurements(name, POLL_TIMEOUT);
        } catch (Exception e) {
            return null;
        }
    }
}
