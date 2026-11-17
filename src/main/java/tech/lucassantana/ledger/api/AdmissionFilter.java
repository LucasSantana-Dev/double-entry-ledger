package tech.lucassantana.ledger.api;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Set;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.http.HttpHeaders;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

/**
 * Bounded admission (specs {@code docs/specs/2026-11-17-admission-control} and
 * {@code docs/specs/2026-11-18-pool-bounds}), with two separate limits:
 *
 * <ul>
 *   <li>writes: at most {@code ledger.admission.max-concurrent} transfers and deposits in progress,
 *       each waiting at most {@code ledger.admission.max-wait} for a slot;</li>
 *   <li>reads: at most {@code ledger.admission.reads.max-concurrent} {@code GET /accounts/{id}} in
 *       progress, each waiting at most {@code ledger.admission.reads.max-wait}.</li>
 * </ul>
 *
 * <p>A request that finds no free slot in time gets a 503 with {@code Retry-After} at once,
 * instead of queueing for a database connection after its client has given up. Each limit is off
 * at 0; with both off the filter is not registered at all. A read never takes a write slot and a
 * write never takes a read slot.
 */
@Component
@ConditionalOnExpression("${ledger.admission.max-concurrent:0} > 0 or ${ledger.admission.reads.max-concurrent:0} > 0")
class AdmissionFilter extends OncePerRequestFilter {

    private static final Set<String> GUARDED = Set.of("/transfers", "/deposits");
    private static final String ACCOUNTS = "/accounts/";
    private static final byte[] OVERLOADED = """
            {"type":"/problems/overloaded","title":"overloaded","status":503,\
            "detail":"Too many requests in progress; retry after the Retry-After delay."}"""
            .getBytes(StandardCharsets.UTF_8);

    /** One limit: null slots means off. */
    private record Limit(Semaphore slots, long maxWaitNanos, Counter rejected) {

        static Limit of(int maxConcurrent, Duration maxWait, String kind, MeterRegistry meters) {
            if (maxConcurrent <= 0) {
                return null;
            }
            return new Limit(new Semaphore(maxConcurrent), maxWait.toNanos(),
                    Counter.builder("ledger.admission.rejected")
                            .description("Requests answered 503 because every admission slot of their kind was taken")
                            .tag("kind", kind)
                            .register(meters));
        }
    }

    private final Limit writes;
    private final Limit reads;

    /** Writes only, as in phase 7. */
    AdmissionFilter(int maxConcurrent, Duration maxWait, MeterRegistry meters) {
        this(maxConcurrent, maxWait, 0, Duration.ZERO, meters);
    }

    @Autowired
    AdmissionFilter(
            @Value("${ledger.admission.max-concurrent:0}") int maxConcurrent,
            @Value("${ledger.admission.max-wait:PT0.1S}") Duration maxWait,
            @Value("${ledger.admission.reads.max-concurrent:0}") int readsMaxConcurrent,
            @Value("${ledger.admission.reads.max-wait:PT0.1S}") Duration readsMaxWait,
            MeterRegistry meters) {
        this.writes = Limit.of(maxConcurrent, maxWait, "write", meters);
        this.reads = Limit.of(readsMaxConcurrent, readsMaxWait, "read", meters);
    }

    @Override
    protected boolean shouldNotFilter(HttpServletRequest request) {
        return limitFor(request) == null;
    }

    private Limit limitFor(HttpServletRequest request) {
        String path = request.getRequestURI().substring(request.getContextPath().length());
        if ("POST".equals(request.getMethod()) && GUARDED.contains(path)) {
            return writes;
        }
        if ("GET".equals(request.getMethod()) && path.startsWith(ACCOUNTS) && path.length() > ACCOUNTS.length()
                && path.indexOf('/', ACCOUNTS.length()) < 0) {
            return reads;
        }
        return null;
    }

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain chain)
            throws ServletException, IOException {
        Limit limit = limitFor(request);
        boolean admitted;
        try {
            admitted = limit.slots().tryAcquire(limit.maxWaitNanos(), TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            admitted = false;
        }
        if (!admitted) {
            limit.rejected().increment();
            response.setStatus(503);
            response.setHeader(HttpHeaders.RETRY_AFTER, "1");
            response.setContentType("application/problem+json");
            response.getOutputStream().write(OVERLOADED);
            return;
        }
        try {
            chain.doFilter(request, response);
        } finally {
            limit.slots().release();
        }
    }
}
