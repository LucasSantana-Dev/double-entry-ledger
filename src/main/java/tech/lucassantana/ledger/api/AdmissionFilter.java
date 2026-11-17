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
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.http.HttpHeaders;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

/**
 * Bounded admission for the write endpoints (spec {@code docs/specs/2026-11-17-admission-control}):
 * at most {@code ledger.admission.max-concurrent} transfers and deposits in progress at a time.
 * A request that finds no free slot within {@code ledger.admission.max-wait} gets a 503 with
 * {@code Retry-After} at once, instead of queueing for a database connection after its client
 * has given up. Off unless {@code max-concurrent} is above 0.
 */
@Component
@ConditionalOnExpression("${ledger.admission.max-concurrent:0} > 0")
class AdmissionFilter extends OncePerRequestFilter {

    private static final Set<String> GUARDED = Set.of("/transfers", "/deposits");
    private static final byte[] OVERLOADED = """
            {"type":"/problems/overloaded","title":"overloaded","status":503,\
            "detail":"Too many transfers in progress; retry after the Retry-After delay."}"""
            .getBytes(StandardCharsets.UTF_8);

    private final Semaphore slots;
    private final long maxWaitNanos;
    private final Counter rejected;

    AdmissionFilter(
            @Value("${ledger.admission.max-concurrent}") int maxConcurrent,
            @Value("${ledger.admission.max-wait:PT0.1S}") Duration maxWait,
            MeterRegistry meters) {
        this.slots = new Semaphore(maxConcurrent);
        this.maxWaitNanos = maxWait.toNanos();
        this.rejected = Counter.builder("ledger.admission.rejected")
                .description("Write requests answered 503 because every admission slot was taken")
                .register(meters);
    }

    @Override
    protected boolean shouldNotFilter(HttpServletRequest request) {
        String path = request.getRequestURI().substring(request.getContextPath().length());
        return !("POST".equals(request.getMethod()) && GUARDED.contains(path));
    }

    @Override
    protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain chain)
            throws ServletException, IOException {
        boolean admitted;
        try {
            admitted = slots.tryAcquire(maxWaitNanos, TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            admitted = false;
        }
        if (!admitted) {
            rejected.increment();
            response.setStatus(503);
            response.setHeader(HttpHeaders.RETRY_AFTER, "1");
            response.setContentType("application/problem+json");
            response.getOutputStream().write(OVERLOADED);
            return;
        }
        try {
            chain.doFilter(request, response);
        } finally {
            slots.release();
        }
    }
}
