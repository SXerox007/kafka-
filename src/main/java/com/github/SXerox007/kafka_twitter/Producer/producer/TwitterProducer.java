package com.github.SXerox007.kafka_twitter.Producer.producer;


import com.github.SXerox007.kafka_twitter.Producer.setup.Setup;
import com.twitter.hbc.core.Client;
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
        setup = new Setup();
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<>(1000);
        Client client = setup.createClient(msgQueue);
        client.connect();

        // on a different thread, or multiple different threads....
        while (!client.isDone()) {
            String msg = null;
            try {
               // String msg = msgQueue.take();
                 msg = msgQueue.poll(10, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
                //stop client
                client.stop();
            }
            if (msg != null) {
                logger.info(msg);
            }

        }
    }
}
