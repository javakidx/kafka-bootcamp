package com.github.jk.elasticsearch

import com.github.jk.elasticsearch.ElasticsearchClientFactory.createClient
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.RequestOptions
import org.elasticsearch.common.xcontent.XContentType
import org.slf4j.LoggerFactory

object ElasticsearchConsumer {
    private val LOGGER = LoggerFactory.getLogger(ElasticsearchConsumer::class.java)

    fun run() {
        val client = createClient()
        val data = "{\"foo\":\"bar\"}"
        val indexRequest = IndexRequest("twitter", "tweets").source(data, XContentType.JSON)
        val response = client.index(indexRequest, RequestOptions.DEFAULT)

        client.close()
        LOGGER.info(String.format("Document indexed, id: %s", response.id))
    }
}

fun main() {
    ElasticsearchConsumer.run()
}