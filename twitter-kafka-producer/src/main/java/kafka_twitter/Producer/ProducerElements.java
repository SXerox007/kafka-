package kafka_twitter.Producer;

import kafka_twitter.constants.constants;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerElements {

    //create properties
    private Properties createProperties(){
        // set default properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, constants.BOOTSTRAP_SERVER);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // safe producer changes
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,"true");
        properties.setProperty(ProducerConfig.ACKS_CONFIG,"all");
        properties.setProperty(ProducerConfig.RETRIES_CONFIG,Integer.toString(10)); // i will do the retries msg to 10 time then ignore the msg
        // We can set the Retries config to Integer.MAX_VALUE
        properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION,"5"); // multiple requests connections


        //high throughput producer bit of latency and CPU usage
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG,"snappy"); //we us data compression for snappy
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG,"20");
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG,Integer.toString(32*1024));

        return properties;
    }

    // Create the producers
    public KafkaProducer<String,String> createProducers(){
        return new KafkaProducer<>(createProperties());
    }
}
