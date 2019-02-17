package com.github.SXerox007.kafka_java_intro.consumers;

import com.github.SXerox007.kafka_java_intro.constants.constants;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;


class ConsumerAssignAndSeekElements{

    //create properties for consumer
    private Properties createProperties(){
        System.out.println("Create Properties");
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, constants.BOOTSTRAP_SERVER);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,constants.OFFSET_EARLIEST);
        return properties;
    }

    // Create the consumers
    public KafkaConsumer<String,String> createConsumer(){
        return new KafkaConsumer<>(createProperties());
    }

}

// Assign and seek used for where to start read message (range)
public class ConsumerAssignAndSeek {

    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(ConsumerAssignAndSeek.class.getName());
        ConsumerAssignAndSeekElements consumerElements = new ConsumerAssignAndSeekElements();
        KafkaConsumer<String,String> consumer = consumerElements.createConsumer();

        //Topic partition with topic name and partition
        // Assign
        TopicPartition partition = new TopicPartition(constants.TOPIC_NAME,constants.PARTITION);
        consumer.assign(Arrays.asList(partition));

        // Seek
        consumer.seek(partition,constants.OFFSET_TO_READ_FROM);

        //any condition
        int temp = 5,i=0;
        boolean read=true;

        while (read) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : records) {
                i++;
                logger.info("\nKey: " + record.key() + " Value: " + record.value());
                logger.info("\nPartition: " + record.partition() + " Offset: " + record.offset());
                if (temp==i){
                    read=false;
                    break;
                }
            }
        }
    }
}
