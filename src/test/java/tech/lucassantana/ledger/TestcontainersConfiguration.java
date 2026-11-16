package tech.lucassantana.ledger;

import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.context.annotation.Bean;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

@TestConfiguration(proxyBeanMethods = false)
public class TestcontainersConfiguration {

    /** Pinned: the locking behavior under test is PostgreSQL's, not a fake's. */
    public static final DockerImageName POSTGRES = DockerImageName.parse("postgres:18-alpine");
    /** A real broker in KRaft mode, the only mode Spring for Apache Kafka 4 supports. */
    public static final DockerImageName KAFKA = DockerImageName.parse("apache/kafka:4.2.2");

    @Bean
    @ServiceConnection
    PostgreSQLContainer postgresContainer() {
        return new PostgreSQLContainer(POSTGRES);
    }

    @Bean
    @ServiceConnection
    KafkaContainer kafkaContainer() {
        return new KafkaContainer(KAFKA);
    }
}
