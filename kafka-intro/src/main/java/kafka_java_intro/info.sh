# https://kafka.apache.org/documentation/#producerconfig

# Test the basic consumer hello sumit
kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic first_topic --group first_group

# Test with basic consumer for key value pair
kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic first_topic --from-beginning --property print.key=true --property key.separator=,