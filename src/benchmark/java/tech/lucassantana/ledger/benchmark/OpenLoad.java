package tech.lucassantana.ledger.benchmark;

import java.io.IOException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.http.HttpTimeoutException;
import java.time.Duration;
import java.util.Arrays;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;
import java.util.function.IntPredicate;
import java.util.function.Supplier;

/**
 * Open-model load generator (ADR 0032): requests leave at a fixed interval for the given rate,
 * whether or not earlier ones were answered, each sent from its own virtual thread.
 *
 * <p>Latency is measured from the time a request was scheduled to leave, not from when it
 * actually left. If the generator falls behind (CPU shared with the server, a GC pause, a slow
 * request builder), the delay is part of the latency of every request it held up, so the
 * generator cannot hide the server's slowness by slowing down with it (coordinated omission,
 * ADR 0030).
 */
final class OpenLoad {

    /** Status codes for requests that got no HTTP answer. */
    static final int TIMEOUT = -1;
    static final int CONNECTION_ERROR = -2;
    /** The generator already had {@code maxInFlight} requests waiting and did not send this one. */
    static final int UNSENT = -3;

    /** Latency of a request with no HTTP answer: slower than any answer. */
    static final long NO_ANSWER = Long.MAX_VALUE;

    /**
     * One step's raw outcome, indexed by request in schedule order.
     *
     * @param latencyNanos answer time minus scheduled time, {@link #NO_ANSWER} without an answer
     * @param status       HTTP status, or {@link #TIMEOUT}, {@link #CONNECTION_ERROR}, {@link #UNSENT}
     * @param sendLagNanos actual send time minus scheduled time: how far behind the generator was
     * @param seconds      wall time from the first scheduled send to the last answer
     */
    record Step(long[] latencyNanos, int[] status, long[] sendLagNanos, double seconds) {

        int scheduled() {
            return status.length;
        }

        long count(IntPredicate statusMatches) {
            long n = 0;
            for (int s : status) {
                if (statusMatches.test(s)) {
                    n++;
                }
            }
            return n;
        }
    }

    private OpenLoad() {
    }

    /**
     * Sends {@code rate x duration} requests at an even interval and returns once every one of
     * them was answered, timed out or failed. {@code requests} is called on the scheduling thread
     * just before each send; the generator sets the timeout.
     */
    static Step run(HttpClient client, Supplier<HttpRequest.Builder> requests, double rate, Duration duration,
            Duration timeout, int maxInFlight) {
        int n = (int) Math.round(rate * duration.toMillis() / 1000.0);
        long[] latency = new long[n];
        int[] status = new int[n];
        long[] lag = new long[n];
        AtomicInteger inFlight = new AtomicInteger();
        double intervalNanos = 1e9 / rate;
        long start = System.nanoTime() + 10_000_000;
        // close() waits for every sender, and each one is bounded by the request timeout. It also
        // publishes the senders' writes to the arrays to this thread.
        try (ExecutorService senders = Executors.newVirtualThreadPerTaskExecutor()) {
            for (int i = 0; i < n; i++) {
                long scheduled = start + (long) (i * intervalNanos);
                sleepUntil(scheduled);
                HttpRequest request = requests.get().timeout(timeout).build();
                if (inFlight.get() >= maxInFlight) {
                    status[i] = UNSENT;
                    latency[i] = NO_ANSWER;
                    lag[i] = System.nanoTime() - scheduled;
                    continue;
                }
                inFlight.incrementAndGet();
                int index = i;
                senders.execute(() -> {
                    lag[index] = System.nanoTime() - scheduled;
                    try {
                        HttpResponse<Void> response = client.send(request, HttpResponse.BodyHandlers.discarding());
                        latency[index] = System.nanoTime() - scheduled;
                        status[index] = response.statusCode();
                    } catch (HttpTimeoutException e) {
                        latency[index] = NO_ANSWER;
                        status[index] = TIMEOUT;
                    } catch (IOException e) {
                        latency[index] = NO_ANSWER;
                        status[index] = CONNECTION_ERROR;
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        latency[index] = NO_ANSWER;
                        status[index] = CONNECTION_ERROR;
                    } finally {
                        inFlight.decrementAndGet();
                    }
                });
            }
        }
        return new Step(latency, status, lag, (System.nanoTime() - start) / 1e9);
    }

    /** Parks until the deadline; returns at once when already late, so lateness shows up as latency. */
    private static void sleepUntil(long deadline) {
        long remaining;
        while ((remaining = deadline - System.nanoTime()) > 0) {
            LockSupport.parkNanos(remaining);
        }
    }

    /** Nearest-rank percentile of nanosecond values, in milliseconds, values above {@code capNanos} reported as the cap. */
    static double percentileMs(long[] values, double p, long capNanos) {
        if (values.length == 0) {
            return -1;
        }
        long[] sorted = values.clone();
        Arrays.sort(sorted);
        int rank = (int) Math.ceil(p / 100.0 * sorted.length);
        long value = Math.min(sorted[Math.max(0, rank - 1)], capNanos);
        return Math.round(value / 100_000.0) / 10.0;
    }
}
