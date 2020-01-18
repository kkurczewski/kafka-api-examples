package pl.kkurczewski;

import org.apache.kafka.clients.consumer.ConsumerRecords;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

import static org.apache.kafka.clients.consumer.ConsumerConfig.*;
import static pl.kkurczewski.ConfigValues.HOST;
import static pl.kkurczewski.ConfigValues.TOPIC;

public class KafkaConsumerExample {

    static final String STRING_DESERIALIZER = "org.apache.kafka.common.serialization.StringDeserializer";

    public static void main(String[] args) {
        Properties consumerProps = new Properties();
        consumerProps.put(BOOTSTRAP_SERVERS_CONFIG, HOST);
        consumerProps.put(GROUP_ID_CONFIG, "test-group");
        consumerProps.put(ENABLE_AUTO_COMMIT_CONFIG, false);
        consumerProps.put(AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(KEY_DESERIALIZER_CLASS_CONFIG, STRING_DESERIALIZER);
        consumerProps.put(VALUE_DESERIALIZER_CLASS_CONFIG, STRING_DESERIALIZER);

        try (var consumer = new org.apache.kafka.clients.consumer.KafkaConsumer<String, String>(consumerProps)) {
            ConsumerRecords<String, String> records;
            do {
                consumer.subscribe(List.of(TOPIC));
                records = consumer.poll(Duration.ofSeconds(5));
                records.forEach(System.out::println);
            } while (records.count() > 0);
        }
        System.out.println("Done");
    }
}
