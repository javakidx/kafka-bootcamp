package com.github.jk.elasticsearch

import com.github.jk.elasticsearch.ComponentFactory.createClient
import com.github.jk.elasticsearch.ComponentFactory.createKafkaConsumer
import com.google.gson.JsonParser
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.elasticsearch.ElasticsearchStatusException
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.RequestOptions
import org.elasticsearch.common.xcontent.XContentType
import org.slf4j.LoggerFactory
import java.time.Duration
import java.util.concurrent.TimeUnit

object ElasticsearchConsumer {
    private val LOGGER = LoggerFactory.getLogger(ElasticsearchConsumer::class.java)

    fun run() {
        val client = createClient()

        val kafkaConsumer = createKafkaConsumer("twitter_tweets", 10)

        var count = 0

        while(true) {
            val messages: ConsumerRecords<String, String> = kafkaConsumer.poll(Duration.ofMillis(100))
            LOGGER.info("Received ${messages.count()} records")
            count += messages.count()

            if (count <= 10 ) {
                continue
            }
            for (message in messages) {
                val id = getIdFromTweet(message.value())
                val indexRequest = IndexRequest("twitter", "tweets", id).source(message.value(), XContentType.JSON)
                try {
                    val response = client.index(indexRequest, RequestOptions.DEFAULT)
                    LOGGER.info(String.format("Document indexed, id: %s", response.id))
                } catch (e: ElasticsearchStatusException) {
                    LOGGER.error("Failed to index message: $message")
                }
            }
            TimeUnit.MILLISECONDS.sleep(10)
            LOGGER.info("Committing offsets...")
            kafkaConsumer.commitAsync()
            LOGGER.info("Committed offsets")
            TimeUnit.MILLISECONDS.sleep(1000)
        }

        Runtime.getRuntime().addShutdownHook(object : Thread() {
            override fun run() {
                client.close()
                LOGGER.info("Elasticsearch client closed.")
            }
        })
    }

    private fun getIdFromTweet(tweet: String): String {
        return JsonParser.parseString(tweet)
            .asJsonObject
            .get("id_str")
            .asString
    }
}

fun main() {
    ElasticsearchConsumer.run()
}