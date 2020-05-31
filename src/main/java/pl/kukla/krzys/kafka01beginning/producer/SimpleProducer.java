package pl.kukla.krzys.kafka01beginning.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * @author Krzysztof Kukla
 */
public class SimpleProducer {
    public static void main(String[] args) {
        Properties kafkaProperties = setProducerProperties();
        ProducerRecord<String, String> producerRecord = produceMessage("first_topic", "message from Java apps");
        sendMessage(producerRecord, kafkaProperties);
    }

    private static void sendMessage(ProducerRecord<String, String> producerRecord, Properties kafkaProperties) {
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(kafkaProperties);

        //this i sending data asynchronous - run in background
        kafkaProducer.send(producerRecord);

        //enforce send data to topic
        kafkaProducer.flush();
        kafkaProducer.close();
    }

    private static Properties setProducerProperties() {
        String localhostPort = "127.0.0.1:9092";

        Properties properties = new Properties();

        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, localhostPort);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return properties;
    }

    private static ProducerRecord<String, String> produceMessage(String topic, String message) {
        return new ProducerRecord<String, String>(topic, message);

    }

}
