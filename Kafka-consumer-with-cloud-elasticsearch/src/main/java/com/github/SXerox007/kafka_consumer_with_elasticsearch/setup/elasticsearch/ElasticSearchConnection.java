package com.github.SXerox007.kafka_consumer_with_elasticsearch.setup.elasticsearch;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;

import static com.github.SXerox007.kafka_consumer_with_elasticsearch.constants.Keys.*;

// bonsai
public class ElasticSearchConnection {

    // get credetial provider
    private CredentialsProvider credentialProvider(){
        CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY,
                new UsernamePasswordCredentials(ELASTIC_SEARCH_CLOUD_USER_NAME,ELASTIC_SEARCH_CLOUD_PASWD));
        return credentialsProvider;
    }

    // client builder
    // which allow insert data to elastic search
    //Basically it's a connnection b/w the bonsai cloud
    private RestHighLevelClient clientBuilder(){
        RestClientBuilder restClientBuilder = RestClient.builder(new HttpHost(ELASTIC_SEARCH_CLOUD_HOST_NAME,
                443,"https")).setHttpClientConfigCallback(httpAsyncClientBuilder ->
                httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialProvider()));
         return new RestHighLevelClient(restClientBuilder);
    }

}
