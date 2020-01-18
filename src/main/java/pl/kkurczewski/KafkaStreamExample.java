package pl.kkurczewski;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static org.apache.kafka.streams.StreamsConfig.*;
import static pl.kkurczewski.ConfigValues.HOST;
import static pl.kkurczewski.ConfigValues.TOPIC;

public class KafkaStreamExample {

    public static void main(String[] args) {

        Properties props = new Properties();
        props.put(APPLICATION_ID_CONFIG, "test-stream");
        props.put(BOOTSTRAP_SERVERS_CONFIG, HOST);
        props.put(DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder();
        builder.<String, String>stream(TOPIC).foreach((key, value) -> System.out.println(key + ": " + value));

        try (var streams = new KafkaStreams(builder.build(), props)) {
            streams.start();
            TimeUnit.SECONDS.sleep(5);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
