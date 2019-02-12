package com.github.SXerox007.kafka_java_intro.Consumers;

import com.github.SXerox007.kafka_java_intro.Constants.constants;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

class ConsumerElements{

    //create properties for consumer
    private Properties createProperties(){
        System.out.println("Create Properties");
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, constants.BOOTSTRAP_SERVER);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,constants.GROUP_ID);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,constants.OFFSET_LATEST);
        return properties;
    }

}


public class Consumer {

    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(Consumer.class.getName());

    }

}
