package com.github.SXerox007.kafka_java_intro;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;


class ProducerCallBackElements {


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
        return new ProducerRecord<String, String>("first_topic","hello sumit");
    }
}


//Producer Kafka
public class ProducerWithCallBack {

    public static void main(String[] args) {
        final Logger logger = LoggerFactory.getLogger(ProducerWithCallBack.class);
        ProducerCallBackElements producersElements = new ProducerCallBackElements();
        //data send is async
        final KafkaProducer<String, String> producer = producersElements.createProducers();
        //send msg with callback
        //callback tells us is there any exception or success
        producer.send(producersElements.createRecord(), new Callback() {
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
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

            }
        });
        //flush and close
        producer.flush();
        producer.close();
    }

}
