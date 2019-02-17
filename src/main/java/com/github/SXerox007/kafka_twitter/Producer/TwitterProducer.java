package com.github.SXerox007.kafka_twitter.Producer;


import com.github.SXerox007.kafka_twitter.Producer.setup.Setup;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

// twitter producer
public class TwitterProducer {

    private Setup setup;

    TwitterProducer(){}

    public static void main(String[] args) {
        new TwitterProducer().run();
    }

    //run
    private void run(){
        setup = new Setup();
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<>(100000);
        setup.createClient(msgQueue).connect();
    }
}
