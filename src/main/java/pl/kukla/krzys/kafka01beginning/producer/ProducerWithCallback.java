package pl.kukla.krzys.kafka01beginning.producer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * @author Krzysztof Kukla
 */
@Slf4j
public class ProducerWithCallback {
    public static void main(String[] args) {
        Properties kafkaProperties = setProducerProperties();
        for(int i=0;i<300;i++) {
            ProducerRecord<String, String> producerRecord = produceMessage("fifth_topic", "Java apps-> "+i);
            sendMessage(producerRecord, kafkaProperties);
        }
    }

    private static void sendMessage(ProducerRecord<String, String> producerRecord, Properties kafkaProperties) {
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(kafkaProperties);

        //this i sending data asynchronous - run in background`
        kafkaProducer.send(producerRecord, onCompletion());

        //enforce send data to topic
        kafkaProducer.flush();
        kafkaProducer.close();
    }

    //Callback onCompletion(RecordMetadata metadata, Exception exception); is called when a record is successfully sent or Exception is thrown
    private static Callback onCompletion() {
        return (metadata, exception) -> {
            if (exception==null) {
                log.debug("Received metadata:" +
                        "\n topic:{}" +
                        "\n partition:{}" +
                        "\n offset:{}" +
                        "\n timestamp:{}",
                    metadata.topic(), metadata.partition(), metadata.offset(), metadata.timestamp()
                );
            }
            else {
                log.error("Error while producing",exception);
            }
        };
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
