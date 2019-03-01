package com.github.SXerox007.kafka_consumer_with_elasticsearch.consumer;

import com.github.SXerox007.kafka_consumer_with_elasticsearch.setup.elasticsearch.ElasticSearchConnection;
import com.google.gson.JsonParser;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;

// consumer thread handler
public class ConsumerThreadHandler implements Runnable {

    private CountDownLatch latch;
    private KafkaConsumer<String,String> consumer;
    private Logger logger;
    private RestHighLevelClient client;
    private JsonParser jsonParser;

     ConsumerThreadHandler(final CountDownLatch latch, final KafkaConsumer<String,String> consumer){
         jsonParser = new JsonParser();
         ElasticSearchConnection elasticSearchConnection = new ElasticSearchConnection();
         this.client =  elasticSearchConnection.clientBuilder();
         this.logger = LoggerFactory.getLogger(ConsumerThreadHandler.class.getName());
         this.latch = latch;
         this.consumer = consumer;
    }

    @Override
    public void run() {
        try {
            while(true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records) {
                    // we have receive all the data we have to push into the elastic search
                    try {
                       String id = pushDataToBonsai(getTwitterIdFromTweet(record.value()),record.value());
                        logger.info("Bonsai Data element ID: " + id);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
        }catch (WakeupException e){
            logger.info("Kafka Consumer Exited");
        } finally {
            try {
                client.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
            consumer.close();
            // tell main method consumer is over
            latch.countDown();

        }

    }

    //shutdown the consumer
    void shutDown(){
        // it will interrupt and make the exception
        consumer.wakeup();
    }


    // push data to bonsai (Elastic Cloud)
    private String pushDataToBonsai(final String id, final String value) throws IOException {
         // create a index reqest
        // it will go to '/twitter/tweets'
        IndexRequest indexRequest = new IndexRequest(
                "twitter",
                "tweets",
                id
        ).source(value, XContentType.JSON);

        // Here we get the index response
        IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
        return  indexResponse.getId();
    }

    // generate the id
    private String getTwitterIdFromTweet(final String tweetJson){
         return jsonParser.parse(tweetJson).getAsJsonObject().get("id_str").toString();
    }
}
