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