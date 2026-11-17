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
        assertThat(meters.counter("ledger.admission.rejected").count()).isEqualTo(1);

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
