# kafka-
Kafka- [Java]

## Producers: 

Step 1:
```$xslt
 //create properties
    private Properties createProperties(){
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,constants.BOOTSTRAP_SERVER);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return properties;
    }
```
Step 2:
```$xslt
// Create the producers
    public KafkaProducer<String,String> createProducers(){
        return new KafkaProducer<String, String>(createProperties());
    }
```

Step 3:
```$xslt
//create record
    public ProducerRecord<String, String> createRecord(){
        return new ProducerRecord<String, String>("first_topic","hello sumit");
    }
}
```


Step 4:
```$xslt
Send msg
producer.send(producersElements.createRecord());
```

###### Note: Here used three properties which are required there are more properties  see link below
https://kafka.apache.org/documentation/#producerconfig

###### Check the msg recived or consumed
```$xslt
kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic first_topic --group first_group
```

## Consumers

Step 1:
```$xslt
 // Create the consumers
    public KafkaConsumer<String,String> createConsumer(){
        return new KafkaConsumer<>(createProperties());
    }
```

Step 2:
```$xslt
 //create properties for consumer
    private Properties createProperties(){
        System.out.println("Create Properties");
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, constants.BOOTSTRAP_SERVER);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,constants.GROUP_ID);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,constants.OFFSET_LATEST);
        return properties;
    }
```

Step 3:
```$xslt
  //subscribe single topic only
    public void subscribeSingleConsumer(){
        createConsumer().subscribe(Collections.singleton(constants.TOPIC_NAME));
    }
    
     //subscribe single topic only
        public void subscribeMultipleConsumer(){
            createConsumer().subscribe(Arrays.asList(constants.TOPIC_NAME));
        }
```

Step 4: 
```$xslt
  // Get records
 ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
```

###### Note: Here used three properties which are required there are more properties  see link below
https://kafka.apache.org/documentation/#consumerconfig


# Twitter-
