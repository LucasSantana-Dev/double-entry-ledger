package tech.lucassantana.ledger.outbox;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/** Created at startup by Spring's {@code KafkaAdmin} when missing. */
@Configuration(proxyBeanMethods = false)
class OutboxTopic {

    /**
     * Three partitions so the transfer-id key spreads load (ADR 0016). One replica fits the
     * single-broker setups this project runs on; a real cluster would use three.
     */
    @Bean
    NewTopic ledgerTransfersTopic(
            @Value("${ledger.outbox.topic}") String topic,
            @Value("${ledger.outbox.topic-replicas:1}") int replicas) {
        return TopicBuilder.name(topic).partitions(3).replicas(replicas).build();
    }
}
