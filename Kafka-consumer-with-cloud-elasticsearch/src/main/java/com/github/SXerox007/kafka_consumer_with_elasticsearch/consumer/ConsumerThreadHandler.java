package com.github.SXerox007.kafka_consumer_with_elasticsearch.consumer;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;

// consumer thread handler
public class ConsumerThreadHandler implements Runnable {


    private CountDownLatch latch;
    private KafkaConsumer<String,String> consumer;
    private Logger logger;

    ConsumerThreadHandler(final CountDownLatch latch, final KafkaConsumer<String,String> consumer){
        this.logger = LoggerFactory.getLogger(ConsumerThreadHandler.class.getName());
        this.latch = latch;
        this.consumer = consumer;
    }

    @Override
    public void run() {

    }

    //shutdown the consumer
    public void shutDown(){
        // it will interrupt and make the exception
        consumer.wakeup();
    }
}
