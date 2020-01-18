package pl.kkurczewski;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.kafka.clients.producer.ProducerConfig.*;
import static pl.kkurczewski.ConfigValues.HOST;
import static pl.kkurczewski.ConfigValues.TOPIC;

public class KafkaProducerExample {

    static final String STRING_SERIALIZER = "org.apache.kafka.common.serialization.StringSerializer";

    public static void main(String[] args) {

        Properties producerProps = new Properties();
        producerProps.put(BOOTSTRAP_SERVERS_CONFIG, HOST);
        producerProps.put(ACKS_CONFIG, "all");
        producerProps.put(KEY_SERIALIZER_CLASS_CONFIG, STRING_SERIALIZER);
        producerProps.put(VALUE_SERIALIZER_CLASS_CONFIG, STRING_SERIALIZER);

        try (var producer = new org.apache.kafka.clients.producer.KafkaProducer<String, String>(producerProps)) {
            RecordMetadata metadata = producer
                    .send(new ProducerRecord<>(TOPIC, "foo", "bar"))
                    .get(10, SECONDS);
            System.out.println(metadata);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            throw new RuntimeException(e);
        }
    }
}
