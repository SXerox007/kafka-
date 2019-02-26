// create the topic
kafka-topics.sh --zookeeper 127.0.0.1:2181 --create --topic twitter_tweets --partitions 6 --replication-factor 1

//start consumer
kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic twitter_tweets --from-beginning

