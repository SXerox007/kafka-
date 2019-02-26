package com.github.SXerox007.kafka_consumer_with_elasticsearch.constants;

public interface Constants {
    String BOOTSTRAP_SERVER = "localhost:9092";
    String GROUP_ID = "kafka_group";
    String OFFSET_NONE = "none";
    String OFFSET_EARLIEST = "earliest";
    String OFFSET_LATEST = "latest";
    String TOPIC_NAME = "twitter_tweets";
    int PARTITION = 0;
    long OFFSET_TO_READ_FROM = 1L;
}
