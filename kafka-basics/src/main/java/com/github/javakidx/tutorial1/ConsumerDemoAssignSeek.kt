package com.github.javakidx.tutorial1

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.slf4j.LoggerFactory
import java.time.Duration
import java.util.Properties
import java.util.concurrent.TimeUnit

object ConsumerDemoAssignSeek {
    @JvmStatic
    fun main(args: Array<String>) {
        val logger = LoggerFactory.getLogger(ConsumerDemoAssignSeek::class.java);
        val bootstrap = "127.0.0.1:9092"
        val topic = "first_topic"
        val groupId = "my-fifth-application"

        val properties = Properties()
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap)
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java.name)
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer::class.java.name)
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId)
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

        val kafkaConsumer: KafkaConsumer<String, String> = KafkaConsumer(properties)

        // assign and seek are mostly used to replay data or fetch a specific message

        // assign
        val partitionToReadFrom = TopicPartition(topic, 0)
        val offsetToReadFrom = 15L
        kafkaConsumer.assign(listOf(partitionToReadFrom))

        // seek
        kafkaConsumer.seek(partitionToReadFrom, offsetToReadFrom)

        val numberOfMessagesToRead = 5
        var keepOnReading = true
        var numberOfMessageRead = 0

        while(keepOnReading) {
            val messages: ConsumerRecords<String, String> = kafkaConsumer.poll(Duration.ofMillis(100))

            for (message in messages) {
                numberOfMessageRead++

                logger.info("""
                    Consumed message from: ${message.topic()}
                    Partition: ${message.partition()}
                    Message: ${message.value()}
                    Key: ${message.key()}
                """.trimIndent())

                if (numberOfMessageRead >= numberOfMessagesToRead) {
                    keepOnReading = false
                    break
                }
            }
            TimeUnit.MILLISECONDS.sleep(100)
        }

        logger.info("Existing the application")
    }
}