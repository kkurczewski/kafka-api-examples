package pl.kkurczewski;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;

import java.util.Properties;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.kafka.streams.StreamsConfig.*;
import static pl.kkurczewski.ConfigValues.BROKER_ADDRESS;
import static pl.kkurczewski.ConfigValues.TOPIC;

public class KafkaStreamExample {

    public static void main(String[] args) {

        Properties properties = new Properties();
        properties.put(BOOTSTRAP_SERVERS_CONFIG, BROKER_ADDRESS);
        properties.put(APPLICATION_ID_CONFIG, "test-stream");
        properties.put(DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder();
        builder.<String, String>stream(TOPIC).foreach((key, value) -> System.out.println(key + ": " + value));

        try (var streams = new KafkaStreams(builder.build(), properties)) {
            streams.start();
            SECONDS.sleep(5);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
