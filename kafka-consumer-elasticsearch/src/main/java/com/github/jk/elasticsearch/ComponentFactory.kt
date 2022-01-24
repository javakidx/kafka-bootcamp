package com.github.jk.elasticsearch

import org.apache.http.HttpHost
import org.apache.http.auth.AuthScope
import org.apache.http.auth.UsernamePasswordCredentials
import org.apache.http.impl.client.BasicCredentialsProvider
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer
import org.elasticsearch.client.RestClient
import org.elasticsearch.client.RestHighLevelClient
import java.util.Properties

object ComponentFactory {

    fun createClient(): RestHighLevelClient {

        //Replace your credentials to connect Elasticsearch here
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

    fun createKafkaConsumer(topic: String, pollRecords: Int): KafkaConsumer<String, String> {
        val bootstrap = "127.0.0.1:9092"
        val groupId = "kafka-demo-application"

        val properties = Properties()
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap)
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java.name)
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java.name)
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId)
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, pollRecords.toString())
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")

        val kafkaConsumer: KafkaConsumer<String, String> = KafkaConsumer(properties)
        kafkaConsumer.subscribe(listOf(topic))

        return kafkaConsumer
    }
}