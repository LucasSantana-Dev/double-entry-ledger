package tech.lucassantana.ledger.api;

import static org.assertj.core.api.Assertions.assertThat;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.springframework.mock.web.MockFilterChain;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;

class AdmissionFilterTest {

    private final SimpleMeterRegistry meters = new SimpleMeterRegistry();
    private final AdmissionFilter filter = new AdmissionFilter(1, Duration.ofMillis(50), meters);

    @Test
    void rejectsWithRetryAfterWhileTheSlotIsTakenAndAdmitsOnceItFrees() throws Exception {
        CountDownLatch inside = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        CompletableFuture<MockHttpServletResponse> first = CompletableFuture.supplyAsync(() -> run(
                new MockFilterChain() {
                    @Override
                    public void doFilter(jakarta.servlet.ServletRequest req, jakarta.servlet.ServletResponse res) {
                        inside.countDown();
                        try {
                            release.await(5, TimeUnit.SECONDS);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                    }
                }));
        assertThat(inside.await(5, TimeUnit.SECONDS)).isTrue();

        MockHttpServletResponse rejected = run(new MockFilterChain());
        assertThat(rejected.getStatus()).isEqualTo(503);
        assertThat(rejected.getHeader("Retry-After")).isEqualTo("1");
        assertThat(rejected.getContentType()).isEqualTo("application/problem+json");
        assertThat(rejected.getContentAsString()).contains("\"type\":\"/problems/overloaded\"");
        assertThat(meters.counter("ledger.admission.rejected", "kind", "write").count()).isEqualTo(1);

        release.countDown();
        assertThat(first.get(5, TimeUnit.SECONDS).getStatus()).isEqualTo(200);
        assertThat(run(new MockFilterChain()).getStatus()).isEqualTo(200);
    }

    @Test
    void leavesReadsAndOtherPathsAlone() throws Exception {
        CountDownLatch inside = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        CompletableFuture.runAsync(() -> run(new MockFilterChain() {
            @Override
            public void doFilter(jakarta.servlet.ServletRequest req, jakarta.servlet.ServletResponse res) {
                inside.countDown();
                try {
                    release.await(5, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }));
        assertThat(inside.await(5, TimeUnit.SECONDS)).isTrue(); // the slot is taken
        try {
            MockHttpServletResponse health = new MockHttpServletResponse();
            filter.doFilter(request("GET", "/actuator/health"), health, new MockFilterChain());
            assertThat(health.getStatus()).isEqualTo(200);
            MockHttpServletResponse account = new MockHttpServletResponse();
            filter.doFilter(request("POST", "/accounts"), account, new MockFilterChain());
            assertThat(account.getStatus()).isEqualTo(200);
        } finally {
            release.countDown();
        }
    }

    @Test
    void releasesTheSlotWhenTheRequestFails() {
        MockFilterChain failing = new MockFilterChain() {
            @Override
            public void doFilter(jakarta.servlet.ServletRequest req, jakarta.servlet.ServletResponse res) {
                throw new IllegalStateException("boom");
            }
        };
        org.assertj.core.api.Assertions.assertThatThrownBy(() -> run(failing)).isInstanceOf(IllegalStateException.class);
        assertThat(run(new MockFilterChain()).getStatus()).isEqualTo(200);
    }

    @Test
    void isOffUnlessMaxConcurrentIsAboveZero() {
        // Boot's conversion service, as in the application, so "PT0.1S" binds to a Duration.
        var runner = new org.springframework.boot.test.context.runner.ApplicationContextRunner()
                .withInitializer(context -> context.getBeanFactory().setConversionService(
                        org.springframework.boot.convert.ApplicationConversionService.getSharedInstance()))
                .withBean(io.micrometer.core.instrument.MeterRegistry.class, SimpleMeterRegistry::new)
                .withUserConfiguration(AdmissionFilter.class);
        runner.run(context -> assertThat(context).doesNotHaveBean(AdmissionFilter.class));
        runner.withPropertyValues("ledger.admission.max-concurrent=0")
                .run(context -> assertThat(context).doesNotHaveBean(AdmissionFilter.class));
        runner.withPropertyValues("ledger.admission.max-concurrent=4")
                .run(context -> assertThat(context).hasSingleBean(AdmissionFilter.class));
    }

    @Test
    void readLimiterRejectsWhenFullAdmitsOnceASlotFreesAndNeverBlocksWrites() throws Exception {
        AdmissionFilter both = new AdmissionFilter(1, Duration.ofMillis(50), 1, Duration.ofMillis(50), meters);
        CountDownLatch inside = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        CompletableFuture<MockHttpServletResponse> first = CompletableFuture.supplyAsync(() -> run(both, "GET",
                "/accounts/1", holding(inside, release)));
        assertThat(inside.await(5, TimeUnit.SECONDS)).isTrue(); // the only read slot is taken

        MockHttpServletResponse rejected = run(both, "GET", "/accounts/2", new MockFilterChain());
        assertThat(rejected.getStatus()).isEqualTo(503);
        assertThat(rejected.getHeader("Retry-After")).isEqualTo("1");
        assertThat(rejected.getContentAsString()).contains("\"type\":\"/problems/overloaded\"");
        assertThat(meters.counter("ledger.admission.rejected", "kind", "read").count()).isEqualTo(1);
        // Separate semaphores: a write still gets its own slot, and listing paths are not reads.
        assertThat(run(both, "POST", "/transfers", new MockFilterChain()).getStatus()).isEqualTo(200);
        assertThat(run(both, "GET", "/accounts/2/statement", new MockFilterChain()).getStatus()).isEqualTo(200);
        assertThat(meters.counter("ledger.admission.rejected", "kind", "write").count()).isZero();

        release.countDown();
        assertThat(first.get(5, TimeUnit.SECONDS).getStatus()).isEqualTo(200);
        assertThat(run(both, "GET", "/accounts/2", new MockFilterChain()).getStatus()).isEqualTo(200);
    }

    @Test
    void readsHoldingTheirSlotsDoNotHoldWritesWhenWritesAreOff() throws Exception {
        AdmissionFilter readsOnly = new AdmissionFilter(0, Duration.ZERO, 1, Duration.ofMillis(50), meters);
        CountDownLatch inside = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        CompletableFuture.runAsync(() -> run(readsOnly, "GET", "/accounts/1", holding(inside, release)));
        assertThat(inside.await(5, TimeUnit.SECONDS)).isTrue();
        try {
            for (int i = 0; i < 3; i++) {
                assertThat(run(readsOnly, "POST", "/transfers", new MockFilterChain()).getStatus()).isEqualTo(200);
            }
        } finally {
            release.countDown();
        }
    }

    @Test
    void readLimiterAloneRegistersTheFilter() {
        var runner = new org.springframework.boot.test.context.runner.ApplicationContextRunner()
                .withInitializer(context -> context.getBeanFactory().setConversionService(
                        org.springframework.boot.convert.ApplicationConversionService.getSharedInstance()))
                .withBean(io.micrometer.core.instrument.MeterRegistry.class, SimpleMeterRegistry::new)
                .withUserConfiguration(AdmissionFilter.class);
        runner.withPropertyValues("ledger.admission.max-concurrent=0", "ledger.admission.reads.max-concurrent=0")
                .run(context -> assertThat(context).doesNotHaveBean(AdmissionFilter.class));
        runner.withPropertyValues("ledger.admission.max-concurrent=0", "ledger.admission.reads.max-concurrent=16",
                        "ledger.admission.reads.max-wait=PT0.1S")
                .run(context -> assertThat(context).hasSingleBean(AdmissionFilter.class));
    }

    /** A chain that signals it is inside, then holds its slot until released. */
    private static MockFilterChain holding(CountDownLatch inside, CountDownLatch release) {
        return new MockFilterChain() {
            @Override
            public void doFilter(jakarta.servlet.ServletRequest req, jakarta.servlet.ServletResponse res) {
                inside.countDown();
                try {
                    release.await(5, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        };
    }

    private static MockHttpServletResponse run(AdmissionFilter filter, String method, String path,
            MockFilterChain chain) {
        MockHttpServletResponse response = new MockHttpServletResponse();
        try {
            filter.doFilter(request(method, path), response, chain);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
        return response;
    }

    /** As the DispatcherServlet mapped to "/" sees it: the whole path is the servlet path. */
    private static MockHttpServletRequest request(String method, String path) {
        MockHttpServletRequest request = new MockHttpServletRequest(method, path);
        request.setServletPath(path);
        return request;
    }

    private MockHttpServletResponse run(MockFilterChain chain) {
        MockHttpServletResponse response = new MockHttpServletResponse();
        try {
            filter.doFilter(request("POST", "/transfers"), response, chain);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
        return response;
    }
}
