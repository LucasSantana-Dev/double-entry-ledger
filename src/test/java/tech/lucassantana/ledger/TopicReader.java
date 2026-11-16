package tech.lucassantana.ledger;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Predicate;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

/** Reads a topic from the beginning as raw strings, outside the application, to see what was really published. */
public final class TopicReader {

    private TopicReader() {
    }

    /** Polls until {@code done} accepts everything read so far, or fails after {@code timeout}. */
    public static List<ConsumerRecord<String, String>> readUntil(
            String bootstrapServers, String topic, Predicate<List<ConsumerRecord<String, String>>> done,
            Duration timeout) {
        Map<String, Object> config = Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers,
                ConsumerConfig.GROUP_ID_CONFIG, "topic-reader-" + UUID.randomUUID(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        List<ConsumerRecord<String, String>> records = new ArrayList<>();
        long deadline = System.nanoTime() + timeout.toNanos();
        try (var consumer = new KafkaConsumer<>(config, new StringDeserializer(), new StringDeserializer())) {
            consumer.subscribe(List.of(topic));
            while (!done.test(records)) {
                if (System.nanoTime() > deadline) {
                    throw new AssertionError("topic " + topic + " did not reach the expected state in " + timeout
                            + "; read " + records.size() + " records");
                }
                consumer.poll(Duration.ofMillis(200)).forEach(records::add);
            }
        }
        return records;
    }
}
