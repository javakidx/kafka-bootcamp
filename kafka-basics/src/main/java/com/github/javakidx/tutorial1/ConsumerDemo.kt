package com.github.javakidx.tutorial1

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer
import org.slf4j.LoggerFactory
import java.time.Duration
import java.util.Properties
import java.util.concurrent.TimeUnit

object ConsumerDemo {
    @JvmStatic
    fun main(args: Array<String>) {
        val logger = LoggerFactory.getLogger(ConsumerDemo::class.java);
        val bootstrap = "127.0.0.1:9092"
        val topic = "first_topic"
        val groupId = "my-fourth-application"

        val properties = Properties()
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap)
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java.name)
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java.name)
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId)
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

        val kafkaConsumer: KafkaConsumer<String, String> = KafkaConsumer(properties)
        kafkaConsumer.subscribe(listOf(topic))

        while(true) {
            val messages: ConsumerRecords<String, String> = kafkaConsumer.poll(Duration.ofMillis(100))

            for (message in messages) {
                logger.info("""
                    Consumed message from: ${message.topic()}
                    Partition: ${message.partition()}
                    Message: ${message.value()}
                    Key: ${message.key()}
                """.trimIndent())
            }
            TimeUnit.MILLISECONDS.sleep(100)
        }
    }
}