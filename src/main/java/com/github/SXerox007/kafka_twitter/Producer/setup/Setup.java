package com.github.SXerox007.kafka_twitter.Producer.setup;

import com.google.common.collect.Lists;
import com.oracle.tools.packager.Log;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;

import java.util.List;
import java.util.concurrent.BlockingQueue;

import static com.github.SXerox007.kafka_twitter.Producer.constants.keys.*;

public class Setup {

    private StatusesFilterEndpoint connectionTwitter() {
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
        List<String> terms = Lists.newArrayList("facebook");
        hosebirdEndpoint.trackTerms(terms);
        return hosebirdEndpoint;
    }

    private Hosts createHosts(){
        return new HttpHosts(Constants.STREAM_HOST);
    }


    private  Authentication oAuth(){
        return new OAuth1(CONSUMER_API, CONSUMER_SECRET, ACCESS_TOKEN, ACCESS_TOKEN_SECRET);
    }

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
