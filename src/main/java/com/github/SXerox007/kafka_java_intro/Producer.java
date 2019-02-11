package com.github.SXerox007.kafka_java_intro;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;


class ProducersElements{

    //create properties
    private Properties createProperties(){
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,constants.BOOTSTRAP_SERVER);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return properties;
    }

    // Create the producers
    public KafkaProducer<String,String> createProducers(){
        return new KafkaProducer<String, String>(createProperties());
    }

    public ProducerRecord<String, String> createRecord(){
        return new ProducerRecord<String, String>("first_topic","hello");
    }
}


//Producer Kafka
public class Producer {

    public static void main(String[] args) {
        ProducersElements producersElements = new ProducersElements();
        //data send is async
        KafkaProducer<String, String> producer = producersElements.createProducers();
        producer.send(producersElements.createRecord());
        //flush and close
        producer.flush();
        producer.close();
    }

}
