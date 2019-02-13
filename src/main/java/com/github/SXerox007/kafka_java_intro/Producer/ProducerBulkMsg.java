package com.github.SXerox007.kafka_java_intro.Producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerBulkMsg {

    public static void main(String[] args) {
        final Logger logger = LoggerFactory.getLogger(ProducerBulkMsg.class);
        ProducersElements producersElements = new ProducersElements();
            //data send is async

            for (int i=0;i<100;i++) {
                final KafkaProducer<String, String> producer = producersElements.createProducers();
            //send msg with callback
            //callback tells us is there any exception or success
            producer.send(producersElements.createKeyValRecord(), (recordMetadata, e) -> {
                if (e == null) {
                    //success
                    logger.info("Topic: " + recordMetadata.topic() +
                            "\n OffSet: " + recordMetadata.hasOffset() +
                            "\n TimeStamp: " + recordMetadata.hasTimestamp() +
                            "\n Partition: " + recordMetadata.partition());

                } else {
                    //error
                    logger.error("Error while sending the data to topic: " + e.getMessage());
                }
            });
            //flush and close
            producer.flush();
            producer.close();
        }

    }
}
