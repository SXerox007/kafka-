package com.github.SXerox007.kafka_twitter.Producer.producer;

import com.github.SXerox007.kafka_twitter.Producer.constants.constants;
import com.github.SXerox007.kafka_twitter.Producer.setup.Setup;
import com.twitter.hbc.core.Client;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

// twitter producer
public class TwitterProducer {

    private Setup setup;
    private Logger logger = LoggerFactory.getLogger(TwitterProducer.class.getName());

    TwitterProducer(){}

    public static void main(String[] args) {
        new TwitterProducer().run();
    }

    //run
    private void run() {
        //setup with twitter
        setup = new Setup();
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<>(1000);
        Client client = setup.createClient(msgQueue);
        client.connect();


        // Create kafka producer
        ProducerElements producerElements = new ProducerElements();
        final KafkaProducer<String, String> producer = producerElements.createProducers();


        // Get message form twitter
        // on a different thread, or multiple different threads....
        while (!client.isDone()) {
            String msg = null;
            try {
               // String msg = msgQueue.take();
                 msg = msgQueue.poll(100, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
                //stop client
                client.stop();
                producer.close();
            }
            if (msg != null) {
                // log the incoming msg from twitter
                logger.info(msg);
                // send data to the consumer
                producer.send(new ProducerRecord<>(constants.TOPIC_NAME, null, msg), (recordMetadata, e) -> {
                    if (e != null){
                     logger.error("Error: " , e.getMessage());
                    }
                });
            }

        }
    }
}
