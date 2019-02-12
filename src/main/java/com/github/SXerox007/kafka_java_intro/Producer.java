package com.github.SXerox007.kafka_java_intro;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;


class ProducersElements{

    //create properties
    private Properties createProperties(){
        System.out.println("Create Properties");
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,constants.BOOTSTRAP_SERVER);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return properties;
    }

    // Create the producers
    public KafkaProducer<String,String> createProducers(){
        System.out.println("Create Producers");
        return new KafkaProducer<String, String>(createProperties());
    }

    public ProducerRecord<String, String> createRecord(){
        System.out.println("Create Record");
        return new ProducerRecord<String, String>("first_topic","hello sumit");
    }
}


//Producer Kafka
public class Producer {

    public static void main(String[] args) {
        ProducerCallBackElements producersElements = new ProducerCallBackElements();
        //data send is async
        KafkaProducer<String, String> producer = producersElements.createProducers();
        producer.send(producersElements.createRecord());
        //flush and close
        producer.flush();
        producer.close();
    }

}
