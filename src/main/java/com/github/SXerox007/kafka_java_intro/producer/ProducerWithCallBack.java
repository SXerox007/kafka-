package com.github.SXerox007.kafka_java_intro.producer;

import org.apache.kafka.clients.producer.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


//Producer Kafka
public class ProducerWithCallBack {

    public static void main(String[] args) {
        final Logger logger = LoggerFactory.getLogger(ProducerWithCallBack.class);
        ProducersElements producersElements = new ProducersElements();
        //data send is async
        final KafkaProducer<String, String> producer = producersElements.createProducers();
        //send msg with callback
        //callback tells us is there any exception or success
        producer.send(producersElements.createRecord(), (recordMetadata, e) -> {
            if (e == null){
                //success
                logger.info("Topic: " +  recordMetadata.topic() +
                "\n OffSet: " + recordMetadata.hasOffset() +
                "\n TimeStamp: " +  recordMetadata.hasTimestamp() +
                "\n Partition: " + recordMetadata.partition());

            }else{
                //error
                logger.error("Error while sending the data to topic: " +  e.getMessage());
            }

        });
        //flush and close
        producer.flush();
        producer.close();
    }

}
