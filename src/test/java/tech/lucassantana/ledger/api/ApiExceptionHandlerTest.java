package tech.lucassantana.ledger.api;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.net.URI;
import java.sql.SQLTransientConnectionException;
import org.junit.jupiter.api.Test;
import org.springframework.http.ProblemDetail;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.CannotGetJdbcConnectionException;
import org.springframework.transaction.CannotCreateTransactionException;

class ApiExceptionHandlerTest {

    private final ApiExceptionHandler handler = new ApiExceptionHandler();

    @Test
    void aPoolTimeoutIsOverloadedWithRetryAfter() {
        var timeout = new SQLTransientConnectionException("HikariPool-1 - Connection is not available");
        for (RuntimeException ex : new RuntimeException[] {
                new CannotCreateTransactionException("Could not open JPA EntityManager",
                        new IllegalStateException("wrapped", timeout)),
                new CannotGetJdbcConnectionException("Failed to obtain JDBC Connection", timeout)}) {
            ResponseEntity<ProblemDetail> answer = handler.noConnection(ex);
            assertThat(answer.getStatusCode().value()).isEqualTo(503);
            assertThat(answer.getHeaders().getFirst("Retry-After")).isEqualTo("1");
            assertThat(answer.getBody().getType()).isEqualTo(URI.create("/problems/overloaded"));
        }
    }

    @Test
    void anyOtherFailureToGetAConnectionStaysAnError() {
        var other = new CannotCreateTransactionException("Could not open JPA EntityManager",
                new IllegalStateException("the database said no"));
        assertThatThrownBy(() -> handler.noConnection(other)).isSameAs(other);
    }
}
