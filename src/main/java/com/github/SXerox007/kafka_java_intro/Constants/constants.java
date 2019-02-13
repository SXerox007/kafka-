package com.github.SXerox007.kafka_java_intro.Constants;

public interface constants {
    String BOOTSTRAP_SERVER = "localhost:9092";
    String GROUP_ID = "test_group";
    String OFFSET_NONE = "none";
    String OFFSET_EARLIEST = "earliest";
    String OFFSET_LATEST = "latest";
    String TOPIC_NAME = "first_topic";
    int PARTITION = 0;
    long OFFSET_TO_READ_FROM = 1L;
}
