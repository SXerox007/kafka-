package com.github.SXerox007.kafka_consumer_with_elasticsearch.setup.elasticsearch.properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import static com.github.SXerox007.kafka_consumer_with_elasticsearch.constants.Constants.*;

public class Properties {

    private java.util.Properties createProperties(){
        System.out.println("Create Properties");
        java.util.Properties properties = new java.util.Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVER);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, OFFSET_LATEST);
        // disable auto commit of offset. By default it will be auto commit. (Manual commit of offset)
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,"false");
        return properties;
    }

    // Create the consumers
    public KafkaConsumer<String,String> createConsumer(){
        return new KafkaConsumer<>(createProperties());
    }

}
