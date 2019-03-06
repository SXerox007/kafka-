package com.github.SXerox007.kafka_consumer_with_elasticsearch.consumer;

import com.github.SXerox007.kafka_consumer_with_elasticsearch.constants.Constants;
import com.github.SXerox007.kafka_consumer_with_elasticsearch.setup.elasticsearch.properties.Properties;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.concurrent.CountDownLatch;

// twitter consumer
public class Consumer {

    public static void main(String[] args) {
        // start the consumer
        new Consumer().run();
    }

    private Consumer(){}

     void run() {
        CountDownLatch countDownLatch = new CountDownLatch(1);
        Properties consumerElements = new Properties();
        KafkaConsumer<String,String> consumer = consumerElements.createConsumer();
        consumer.subscribe(Arrays.asList(Constants.TOPIC_NAME));
        //Make runnable of thread
        Runnable runnable = new ConsumerThreadHandler(countDownLatch,consumer);
        // create thread
        Thread thread = new Thread(runnable);
        //start the thread
        thread.start();

        //  shutdown
        Runtime.getRuntime().addShutdownHook(new Thread(()->{
            ((ConsumerThreadHandler) runnable).shutDown();
            try {
                countDownLatch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }));

        // not exit the app while running so need to await
        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }
}
