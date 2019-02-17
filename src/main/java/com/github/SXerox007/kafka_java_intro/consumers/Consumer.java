package com.github.SXerox007.kafka_java_intro.consumers;

import com.github.SXerox007.kafka_java_intro.constants.constants;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

class ConsumerElements{

    //create properties for consumer
    private Properties createProperties(){
        System.out.println("Create Properties");
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, constants.BOOTSTRAP_SERVER);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,constants.GROUP_ID);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,constants.OFFSET_LATEST);
        return properties;
    }

    // Create the consumers
    public KafkaConsumer<String,String> createConsumer(){
        return new KafkaConsumer<>(createProperties());
    }

    //subscribe single topic only
    public void subscribeSingleConsumer(){
        createConsumer().subscribe(Collections.singleton(constants.TOPIC_NAME));
    }

    //subscribe single topic only
    public void subscribeMultipleConsumer(){
        createConsumer().subscribe(Arrays.asList(constants.TOPIC_NAME));
    }


}


public class Consumer {

    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(Consumer.class.getName());
        ConsumerElements consumerElements = new ConsumerElements();
        KafkaConsumer<String,String> consumer = consumerElements.createConsumer();
        consumer.subscribe(Arrays.asList(constants.TOPIC_NAME));

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

            for (ConsumerRecord<String, String> record : records) {
                logger.info("\nKey: " + record.key() + " Value: " + record.value());
                logger.info("\nPartition: " + record.partition() + " Offset: " + record.offset());
            }
        }
    }

}
