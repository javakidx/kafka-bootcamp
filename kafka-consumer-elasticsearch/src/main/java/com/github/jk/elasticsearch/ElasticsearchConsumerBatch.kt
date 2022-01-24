package com.github.jk.elasticsearch

import com.github.jk.elasticsearch.ComponentFactory.createClient
import com.github.jk.elasticsearch.ComponentFactory.createKafkaConsumer
import com.google.gson.JsonParser
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.elasticsearch.action.bulk.BulkRequest
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.RequestOptions
import org.elasticsearch.common.xcontent.XContentType
import org.slf4j.LoggerFactory
import java.time.Duration
import java.util.concurrent.TimeUnit

object ElasticsearchConsumerBatch {
    private val LOGGER = LoggerFactory.getLogger(ElasticsearchConsumer::class.java)

    fun run() {
        val client = createClient()

        val kafkaConsumer = createKafkaConsumer("twitter_tweets", 100)

        while(true) {
            val messages: ConsumerRecords<String, String> = kafkaConsumer.poll(Duration.ofMillis(100))
            LOGGER.info("Received ${messages.count()} records")
            val count = messages.count()

            if (count <= 0) {
                continue
            }

            val bulkRequest = BulkRequest()

            for (message in messages) {
                val id = try {
                    getIdFromTweet(message.value())
                } catch (e: NullPointerException) {
                    LOGGER.error("Failed to get id, message: ${message.value()}")
                }
                val indexRequest = IndexRequest("twitter", "tweets", id as String?).source(message.value(), XContentType.JSON)
                bulkRequest.add(indexRequest)

                LOGGER.info("Document added to bulk, id: $id")
            }
            client.bulk(bulkRequest, RequestOptions.DEFAULT)

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
    ElasticsearchConsumerBatch.run()
}