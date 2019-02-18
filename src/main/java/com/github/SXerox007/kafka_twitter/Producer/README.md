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
### Create Producer and send msg

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


### For test the cli consumers
```$xslt

kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic twitter_tweets --from-beginning

```
