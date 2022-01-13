package com.github.javakidx.twitter

import com.google.common.collect.Lists
import com.twitter.hbc.ClientBuilder
import com.twitter.hbc.core.Client
import com.twitter.hbc.core.Constants
import com.twitter.hbc.core.Hosts
import com.twitter.hbc.core.HttpHosts
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint
import com.twitter.hbc.core.processor.StringDelimitedProcessor
import com.twitter.hbc.httpclient.auth.Authentication
import com.twitter.hbc.httpclient.auth.OAuth1
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer
import org.slf4j.LoggerFactory
import java.util.*
import java.util.concurrent.BlockingQueue
import java.util.concurrent.LinkedBlockingQueue


class TwitterProducer {
    private val logger = LoggerFactory.getLogger(TwitterProducer::class.java)

    /**
     * The consumer API key from Twitter
     */
    private val consumerKey = ""
    /**
     * The consumer API secret key from Twitter
     */
    private val consumerSecret = ""
    /**
     * The access token from Twitter
     */
    private val token = ""
    /**
     * The access token secret from Twitter
     */
    private val secret = ""
    private val terms: List<String> = Lists.newArrayList("bitcoin")

    fun run() {
        logger.info("Start")

        /** Set up your blocking queues: Be sure to size these properly based on expected TPS of your stream  */
        val msgQueue: BlockingQueue<String> = LinkedBlockingQueue(1000)
        // on a different thread, or multiple different threads....
        val client = createTwitterClient(msgQueue)
        client.connect()

        val producer = createKafkaProducer()

        Runtime.getRuntime().addShutdownHook(object : Thread() {
            override fun run() {
                logger.info("Stopping application")
                logger.info("Stopping twitter client")
                client.stop()

                logger.info("Closing producer")
                producer.close()
            }
        })

        while (!client.isDone) {
            val msg: String = msgQueue.take()
            logger.info(msg)
            producer.send(ProducerRecord("twitter_tweets", null, msg))
        }
        logger.info("End of application")
    }

    private fun createTwitterClient(msgQueue: BlockingQueue<String>): Client {
        /** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
        /** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth)  */
        val hosebirdHosts: Hosts = HttpHosts(Constants.STREAM_HOST)
        val hosebirdEndpoint = StatusesFilterEndpoint()
        // Optional: set up some followings and track terms
        // Optional: set up some followings and track terms

        hosebirdEndpoint.trackTerms(terms)
        // These secrets should be read from a config file

        // These secrets should be read from a config file
        val hosebirdAuth: Authentication = OAuth1(consumerKey, consumerSecret, token, secret)

        val builder = ClientBuilder()
                .name("Hosebird-Client-01") // optional: mainly for the logs
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(StringDelimitedProcessor(msgQueue))

        return builder.build()
    }

    private fun createKafkaProducer(): KafkaProducer<String, String> {
        val bootstrapServers = "127.0.0.1:9092"

        val properties = Properties()
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java.name)
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java.name)

        // Set safe producer
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true")
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, Int.MAX_VALUE.toString())
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all")
        properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5")

        // high throughput producer
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy")
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20")
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, (32 * 1024).toString())

        return KafkaProducer(properties)
    }
}

fun main() {
    TwitterProducer().run()
}