# Twitter-with-kafka


Get the data from twitter in continues and push data to kafka
and do the elastic search for test

## Producer

### Setup Twitter :
```$xslt
public class Setup {

    private StatusesFilterEndpoint connectionTwitter() {
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
        List<String> terms = Lists.newArrayList("facebook");
        hosebirdEndpoint.trackTerms(terms);
        return hosebirdEndpoint;
    }
    //create host
    private Hosts createHosts(){
        return new HttpHosts(Constants.STREAM_HOST);
    }

    // oAuth
    private  Authentication oAuth(){
        return new OAuth1(CONSUMER_API, CONSUMER_SECRET, ACCESS_TOKEN, ACCESS_TOKEN_SECRET);
    }

    // create the client
    public Client createClient(BlockingQueue<String> msgQueue){
        ClientBuilder builder = new ClientBuilder()
                .name("Hosebird-Client-01")                              // optional: mainly for the logs
                .hosts(createHosts())
                .authentication(oAuth())
                .endpoint(connectionTwitter())
                .processor(new StringDelimitedProcessor(msgQueue));
        return builder.build();

    }


}


```

#### Producer Properties

```$xslt

   // set default properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, constants.BOOTSTRAP_SERVER);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // safe producer changes
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,"true");
        properties.setProperty(ProducerConfig.ACKS_CONFIG,"all");
        properties.setProperty(ProducerConfig.RETRIES_CONFIG,Integer.toString(10)); // i will do the retries msg to 10 time then ignore the msg
        // We can set the Retries config to Integer.MAX_VALUE
        properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION,"5"); // multiple requests connections
        
        
        //high throughput producer bit of latency and CPU usage
         properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG,"snappy"); //we us data compression for snappy
         properties.setProperty(ProducerConfig.LINGER_MS_CONFIG,"20");
         properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG,Integer.toString(32*1024));

```



### Create Producer and send msg :

```$xslt
    // Create the producers
    public KafkaProducer<String,String> createProducers(){
        return new KafkaProducer<>(createProperties());
    }
    
    //send 
   
    producer.send(new ProducerRecord<>(constants.TOPIC_NAME, null, msg), (recordMetadata, e) -> {
        if (e != null){
           logger.error("Error: " + e.getMessage());
        }
    });
```


### For test the cli consumers :
```$xslt
// create the topic 
kafka-topics.sh --zookeeper 127.0.0.1:2181 --create --topic twitter_tweets --partitions 6 --replication-factor 1

//start consumer
kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic twitter_tweets --from-beginning

```
