package com.github.SXerox007.kafka_java_intro.Consumers;

import com.github.SXerox007.kafka_java_intro.Constants.constants;

import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.concurrent.CountDownLatch;

public class ConsumerWithThreadHandler {
    public static void main(String[] args) {
        new ConsumerWithThreadHandler().run();
    }
    private ConsumerWithThreadHandler(){}

    public void run(){
        CountDownLatch countDownLatch = new CountDownLatch(1);
        ConsumerElements consumerElements = new ConsumerElements();
        KafkaConsumer<String,String> consumer = consumerElements.createConsumer();
        consumer.subscribe(Arrays.asList(constants.TOPIC_NAME));
        //Make runnable of thread
        ConsumerThread runnable = new ConsumerThread(countDownLatch,consumer);
        // create thread
        Thread thread = new Thread(runnable);
        //start the thread
        thread.start();

        //  shutdown
        Runtime.getRuntime().addShutdownHook(new Thread(()->{
            runnable.shutDown();
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
