package com.github.javakidx.tutorial1

import org.apache.kafka.clients.producer.*
import org.apache.kafka.common.serialization.StringSerializer
import org.slf4j.LoggerFactory
import java.util.*

object ProducerDemoWithCallback {
    @JvmStatic
    fun main(args: Array<String>) {
        val logger = LoggerFactory.getLogger(ProducerDemoWithCallback::class.java)
        val topic = "first_topic"
        val bootstrapServers = "127.0.0.1:9092"

        val properties = Properties()
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java.name)
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java.name)
        val producer: KafkaProducer<String, String> = KafkaProducer<String, String>(properties)
        val message: ProducerRecord<String, String> = ProducerRecord<String, String>(topic, "Hello World!")

        producer.send(message) { recordMetadata: RecordMetadata?, e: Exception? ->
            if (e == null) {
                logger.info("""Received new metadata from: ${recordMetadata?.topic()}
                    Partition: ${recordMetadata?.partition()}
                    Offset: ${recordMetadata?.offset()}
                    Timestamp: ${recordMetadata?.timestamp()}
                """.trimMargin())
            } else {
                logger.error("Error thrown!", e)
            }
        }

        producer.flush()
        producer.close()
    }
}