package pl.kkurczewski;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Instant;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.kafka.clients.producer.ProducerConfig.ACKS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static pl.kkurczewski.ConfigValues.BROKER_ADDRESS;
import static pl.kkurczewski.ConfigValues.TOPIC;

public class KafkaProducerExample {

    public static void main(String[] args) {
        Map<String, Object> properties = Map.of(
                BOOTSTRAP_SERVERS_CONFIG, BROKER_ADDRESS,
                ACKS_CONFIG, "all"
        );

        var keySerializer = new StringSerializer();
        var valueSerializer = new StringSerializer();
        var producer = new KafkaProducer<>(properties, keySerializer, valueSerializer);

        try (producer) {
            for (int i = 0; i < 3; i++) {
                var metadata = producer
                        .send(new ProducerRecord<>(TOPIC, "foo-" + Instant.now(), "bar"))
                        .get(100, MILLISECONDS);
                System.out.println(metadata);
            }
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            throw new RuntimeException(e);
        }
    }
}
