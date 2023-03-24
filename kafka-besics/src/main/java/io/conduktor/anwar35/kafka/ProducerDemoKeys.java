package io.conduktor.anwar35.kafka;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoKeys {
    private static final Logger log = LoggerFactory.getLogger(ProducerDemoKeys.class.getSimpleName());
    public static void main(String[] args) {
        log.info("I am a kafka producer!");

        // create producer properties
        Properties properties = new Properties();

        // connect to conduktor playground
        properties.setProperty("bootstrap.servers", "cluster.playground.cdkt.io:9092");
        properties.setProperty("security.protocol", "SASL_SSL");
        properties.setProperty("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"1zep1U2Mn1Agtk9hfZwTNx\" password=\"eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpc3MiOiJodHRwczovL2F1dGguY29uZHVrdG9yLmlvIiwic291cmNlQXBwbGljYXRpb24iOiJhZG1pbiIsInVzZXJNYWlsIjpudWxsLCJwYXlsb2FkIjp7InZhbGlkRm9yVXNlcm5hbWUiOiIxemVwMVUyTW4xQWd0azloZlp3VE54Iiwib3JnYW5pemF0aW9uSWQiOjcxNzAyLCJ1c2VySWQiOjgzMTYxLCJmb3JFeHBpcmF0aW9uQ2hlY2siOiI0YjhiM2E1MS1mNDVmLTQyMDYtYTY3Yy1jYjQ5OWRkOGU1ZWUifX0.2TmXRI-zZ_7y7y2H0gi_vMGs3NK39TOA2ijEUYtKrX8\";");
        properties.setProperty("sasl.mechanism", "PLAIN");

        // set producer properties
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());


        // create producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        for (int j = 0; j < 2; j++) {
            for (int i = 0; i < 10; i++) {
                String topic = "demo_java";
                String key = "id_" + i;
                String value = "hello world" + i;

                // create producer record
                ProducerRecord<String, String> producerRecord =
                        new ProducerRecord<>(topic, key, value);

                // send data
                producer.send(producerRecord, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception e) {
                        if (e == null) {
                            log.info("Key: " +key + " | Partition: " + metadata.partition());
                        } else {
                            log.error("Error while producing", e);
                        }
                    }
                });
            }

            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }


        // tell the producer to send all data and block until done -- synchronous
        producer.flush();

        // flush and close producer
        producer.close();

    }
}
