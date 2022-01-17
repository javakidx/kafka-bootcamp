package com.github.jk.elasticsearch

import org.apache.http.HttpHost
import org.apache.http.auth.AuthScope
import org.apache.http.auth.UsernamePasswordCredentials
import org.apache.http.impl.client.BasicCredentialsProvider
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder
import org.elasticsearch.client.RestClient
import org.elasticsearch.client.RestHighLevelClient

object ElasticsearchClientFactory {

    fun createClient(): RestHighLevelClient {

        //Replace your credentials here
        val hostname = ""
        val username = ""
        val password = ""

        val credentialsProvider = BasicCredentialsProvider()
        credentialsProvider.setCredentials(AuthScope.ANY, UsernamePasswordCredentials(username, password))

        val restClientBuilder = RestClient.builder(HttpHost(hostname, 443, "https"))
            .setHttpClientConfigCallback { httpClientBuilder: HttpAsyncClientBuilder ->
                httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider)
            }

        return RestHighLevelClient(restClientBuilder)
    }
}