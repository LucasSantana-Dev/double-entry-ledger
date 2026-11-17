package tech.lucassantana.ledger.benchmark;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

/**
 * Samples the application's database pool and request queue once a second during a step (spec
 * {@code docs/specs/2026-11-17-admission-control}, scope 1): threads waiting for a connection,
 * the mean connection acquire time since the last answered read, and requests in progress. Phase
 * 8 (spec {@code docs/specs/2026-11-18-pool-bounds}) adds the outbox backlog, the age of its
 * oldest row and the statement consumer's lag; without a broker these read 0 or null. The reads
 * go out together and the sample is stamped when they finished; a read the swamped
 * server does not answer within a second is recorded as missing (null).
 *
 * <p>The consumer lag is read by the harness from the broker, not from the application: the sum
 * over the partitions of {@code ledger.transfers} of the log end offset minus the statement
 * group's committed offset, that is the events published but not yet applied to the statement.
 * The application's {@code kafka.consumer.fetch.manager.records.lag.max} meter was dropped: it is
 * a maximum over the client's fetches in a sample window, not a backlog, and in calibration run
 * 44171716101 it read 709,528 while the reconciliation reported 328,797 transfers missing from the
 * statement, more than the transfers produced in the step.
 */
final class PoolSampler {

    /** One second of a step; a field is null when the application did not answer in time. */
    record Sample(double secondsIntoStep, Double pending, Double acquireMeanMs, Double active, Double outboxPending,
            Double outboxOldestAgeSeconds, Double consumerLag) {
    }

    /** The outbox topic and the statement consumer's group ({@code ledger.statement.group-id}'s default). */
    static final String TOPIC = "ledger.transfers";
    static final String GROUP = "ledger-statement";

    private static final Duration POLL_TIMEOUT = Duration.ofSeconds(1);

    private final AppProcess app;
    private final Admin kafka;
    private final List<Sample> samples = new ArrayList<>();
    private final Thread thread;
    private final long startNanos = System.nanoTime();
    private volatile boolean running = true;

    private PoolSampler(AppProcess app, Admin kafka) {
        this.app = app;
        this.kafka = kafka;
        this.thread = Thread.ofVirtual().start(this::loop);
    }

    /** {@code kafka} is null without a broker; the consumer lag then reads null. */
    static PoolSampler start(AppProcess app, Admin kafka) {
        return new PoolSampler(app, kafka);
    }

    /**
     * Events on the topic not yet committed by the statement group, summed over partitions; a
     * partition the group never committed counts from offset 0. Null when the broker does not
     * answer within a second.
     */
    static Double consumerLag(Admin kafka) {
        try {
            Map<TopicPartition, OffsetAndMetadata> committed = kafka.listConsumerGroupOffsets(GROUP)
                    .partitionsToOffsetAndMetadata().get(1, TimeUnit.SECONDS);
            Map<TopicPartition, OffsetSpec> latest = new java.util.HashMap<>();
            kafka.describeTopics(List.of(TOPIC)).allTopicNames().get(1, TimeUnit.SECONDS).get(TOPIC).partitions()
                    .forEach(p -> latest.put(new TopicPartition(TOPIC, p.partition()), OffsetSpec.latest()));
            long lag = 0;
            for (var end : kafka.listOffsets(latest).all().get(1, TimeUnit.SECONDS).entrySet()) {
                OffsetAndMetadata done = committed.get(end.getKey());
                lag += end.getValue().offset() - (done == null ? 0 : done.offset());
            }
            return (double) lag;
        } catch (Exception e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            return null;
        }
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
            var outboxRead = java.util.concurrent.CompletableFuture.supplyAsync(
                    () -> value("ledger.outbox.pending", "VALUE"), Thread::startVirtualThread);
            var ageRead = java.util.concurrent.CompletableFuture.supplyAsync(
                    () -> value("ledger.outbox.oldest.pending.age", "VALUE"), Thread::startVirtualThread);
            var lagRead = java.util.concurrent.CompletableFuture.supplyAsync(
                    () -> kafka == null ? null : consumerLag(kafka), Thread::startVirtualThread);
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
            samples.add(new Sample(Math.round(at * 10) / 10.0, pending, acquireMean, active, outboxRead.join(),
                    ageRead.join(), lagRead.join()));
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
