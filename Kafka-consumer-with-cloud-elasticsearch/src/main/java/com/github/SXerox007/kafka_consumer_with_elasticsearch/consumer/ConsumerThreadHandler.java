package com.github.SXerox007.kafka_consumer_with_elasticsearch.consumer;

import com.github.SXerox007.kafka_consumer_with_elasticsearch.setup.elasticsearch.ElasticSearchConnection;
import com.google.gson.JsonParser;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
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
    private BulkRequest bulkRequest;
    private BulkResponse bulkResponse;
     ConsumerThreadHandler(final CountDownLatch latch, final KafkaConsumer<String,String> consumer){
         // performance improvement using bulk request
          this.bulkRequest = new BulkRequest();
         // json parser
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
                logger.info("Size of record::" + records.count());
                for (ConsumerRecord<String, String> record : records) {
                    // we have receive all the data we have to push into the elastic search
                    try {
                       String id = pushDataToBonsai(getTwitterIdFromTweet(record.value()),record);
                        //logger.info("Bonsai Data element ID: " + id);
                        //logger.info("Bonsai Data element ID but not used in Elastic search just for Example: " + id);
                    } catch (Exception e) {
                        // Bad data
                        logger.error("Bad Data Kafka: " + record.value());
                        e.printStackTrace();
                    }
                }
                if(records.count()>0) {
                    try {
                        bulkResponse = client.bulk(bulkRequest, RequestOptions.DEFAULT);
                        logger.info("Bulk Response:" + bulkResponse);
                    } catch (IOException e) {
                        logger.error("Bulk Error: " + e.getMessage());
                        e.printStackTrace();
                    }
                    // this will commit the data // we will not use auto-commit
                    consumer.commitSync();
                }
            }
        }catch (WakeupException e){
            logger.info("Kafka Consumer Exited");
        }  finally {
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
    private String pushDataToBonsai(final String id, final ConsumerRecord record) throws IOException {
         // create a index reqest
        // it will go to '/twitter/tweets'
        IndexRequest indexRequest = new IndexRequest(
                "twitter",
                "tweets",
                id
        ).source(record.value(), XContentType.JSON);

        // Here we get the index response
        // for normal push Data
        //return pushNormalData(indexRequest);
        return pushBulkData(indexRequest,record);

        //connnectToPhone
    }


    // performance data
    private String pushBulkData(final IndexRequest indexRequest,final ConsumerRecord record){
        bulkRequest.add(indexRequest);
        return record.topic() + "_" + record.partition() + "_" + record.offset();
    }

    // normal data
    private String pushNormalData(final IndexRequest indexRequest) throws IOException {
        IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
        return  indexResponse.getId();
    }

    // generate the id
    private String getTwitterIdFromTweet(final String tweetJson){
         return jsonParser.parse(tweetJson).getAsJsonObject().get("id_str").toString();
    }
}
