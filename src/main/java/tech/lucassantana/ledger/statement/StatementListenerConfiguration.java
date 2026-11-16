package tech.lucassantana.ledger.statement;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

@Configuration(proxyBeanMethods = false)
class StatementListenerConfiguration {

    /**
     * Retries a failed record every 500 ms without a limit (ADR 0017): a database outage pauses
     * the statement instead of skipping events. Deserialization failures are on the error
     * handler's not-retryable list, so they are logged and skipped; reconciliation reports them.
     * Spring Boot applies this bean to the listener container factory.
     */
    @Bean
    CommonErrorHandler statementErrorHandler() {
        return new DefaultErrorHandler(new FixedBackOff(500, FixedBackOff.UNLIMITED_ATTEMPTS));
    }
}
