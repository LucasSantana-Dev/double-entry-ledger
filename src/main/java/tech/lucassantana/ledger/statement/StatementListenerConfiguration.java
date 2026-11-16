package tech.lucassantana.ledger.statement;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.net.ConnectException;
import java.sql.SQLException;
import java.sql.SQLRecoverableException;
import java.sql.SQLTransientException;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.dao.RecoverableDataAccessException;
import org.springframework.dao.TransientDataAccessException;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.transaction.CannotCreateTransactionException;
import org.springframework.transaction.TransactionSystemException;
import org.springframework.util.backoff.BackOff;
import org.springframework.util.backoff.FixedBackOff;

@Configuration(proxyBeanMethods = false)
class StatementListenerConfiguration {

    private static final Logger log = LoggerFactory.getLogger(StatementListenerConfiguration.class);

    /** Database unreachable: wait for it as long as it takes, the event is fine (ADR 0017). */
    private static final BackOff DATABASE_DOWN = new FixedBackOff(1_000, FixedBackOff.UNLIMITED_ATTEMPTS);
    /** Anything else fails the same way every time: three more tries, then the dead letter topic. */
    private static final BackOff POISON = new FixedBackOff(500, 3);

    /** Same partition count as the source topic: the recoverer keeps a record's partition. */
    @Bean
    NewTopic ledgerTransfersDeadLetterTopic(
            @Value("${ledger.outbox.topic}") String topic,
            @Value("${ledger.outbox.topic-replicas:1}") int replicas) {
        return TopicBuilder.name(topic + ".DLT").partitions(3).replicas(replicas).build();
    }

    /**
     * Retries a record while the database is unreachable, so an outage pauses the statement
     * instead of dead-lettering events. A record that fails for any other reason (an event that
     * deserializes but cannot be applied) gets three more tries and then goes to
     * {@code <topic>.DLT} with the failure in its headers, and the partition moves on.
     * Deserialization failures skip the retries. Reconciliation reports the transfer as missing
     * from the statement. Spring Boot applies this bean to the listener container factory.
     */
    @Bean
    CommonErrorHandler statementErrorHandler(KafkaTemplate<Object, Object> kafka, MeterRegistry meters) {
        Counter deadLettered = meters.counter("ledger.statement.dead-lettered");
        // Named explicitly: Spring for Apache Kafka 4 defaults to a "-dlt" suffix.
        var deadLetter = new DeadLetterPublishingRecoverer(kafka,
                (record, error) -> new TopicPartition(record.topic() + ".DLT", record.partition()));
        var handler = new DefaultErrorHandler((record, error) -> {
            log.error("statement dead-lettered record {}-{}@{}: {}",
                    record.topic(), record.partition(), record.offset(), error.toString());
            deadLetter.accept(record, error);
            deadLettered.increment();
        }, POISON);
        handler.setBackOffFunction((record, error) -> databaseDown(error) ? DATABASE_DOWN : POISON);
        return handler;
    }

    private static boolean databaseDown(Throwable error) {
        for (Throwable t = error; t != null; t = t.getCause()) {
            if (t instanceof TransientDataAccessException
                    || t instanceof RecoverableDataAccessException
                    || t instanceof DataAccessResourceFailureException
                    || t instanceof CannotCreateTransactionException
                    || t instanceof TransactionSystemException
                    || t instanceof SQLTransientException
                    || t instanceof SQLRecoverableException
                    || t instanceof ConnectException
                    // SQLSTATE class 08: connection exception, as PostgreSQL's driver reports a lost link.
                    || (t instanceof SQLException sql && sql.getSQLState() != null
                            && sql.getSQLState().startsWith("08"))) {
                return true;
            }
        }
        return false;
    }
}
