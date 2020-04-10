package pl.kkurczewski;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.apache.kafka.clients.consumer.ConsumerConfig.*;
import static pl.kkurczewski.ConfigValues.BROKER_ADDRESS;
import static pl.kkurczewski.ConfigValues.TOPIC;

public class KafkaConsumerExample {

    public static void main(String[] args) {
        Map<String, Object> properties = Map.of(
                BOOTSTRAP_SERVERS_CONFIG, BROKER_ADDRESS,
                GROUP_ID_CONFIG, "test-group-" + Instant.now(),
                ENABLE_AUTO_COMMIT_CONFIG, false,
                AUTO_OFFSET_RESET_CONFIG, "earliest"
        );

        var keySerializer = new StringDeserializer();
        var valueSerializer = new StringDeserializer();
        var consumer = new KafkaConsumer<>(properties, keySerializer, valueSerializer);

        List<ConsumerRecord<String, String>> records = getRecords(TOPIC, consumer, Duration.ofSeconds(5));
        records.forEach(System.out::println);
        System.out.println("Consumed " + records.size() + " events");
    }

    private static List<ConsumerRecord<String, String>> getRecords(String topic, KafkaConsumer<String, String> consumer, Duration timeout) {
        List<ConsumerRecord<String, String>> result = new ArrayList<>();
        try (consumer) {
            consumer.subscribe(List.of(topic));
            ConsumerRecords<String, String> records;
            do {
                records = consumer.poll(timeout);
                records.forEach(result::add);
            } while (records.count() > 0);
        }
        return result;
    }
}
