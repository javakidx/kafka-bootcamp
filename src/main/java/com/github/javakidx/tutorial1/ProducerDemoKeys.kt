package com.github.javakidx.tutorial1

import org.apache.kafka.clients.producer.*
import org.apache.kafka.common.serialization.StringSerializer
import org.slf4j.LoggerFactory
import java.util.*

object ProducerDemoKeys {
    @JvmStatic
    fun main(args: Array<String>) {
        val logger = LoggerFactory.getLogger(ProducerDemoKeys::class.java)
        val topic = "first_topic"
        val bootstrapServers = "127.0.0.1:9092"

        val properties = Properties()
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java.name)
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java.name)
        val producer: KafkaProducer<String, String> = KafkaProducer<String, String>(properties)

        for (i in 1..10) {
            val value = "Hello World! $i"
            val key = "id_${i % 3}" //The same key will be sent to the same partition
            val message: ProducerRecord<String, String> = ProducerRecord<String, String>(topic, key, value)

            producer.send(message) { recordMetadata: RecordMetadata?, e: Exception? ->
                if (e == null) {
                    logger.info("""
                        Sent message: $value
                        Kay: $key
                        Received new metadata from: ${recordMetadata?.topic()}
                        Partition: ${recordMetadata?.partition()}
                        Offset: ${recordMetadata?.offset()}
                        Timestamp: ${recordMetadata?.timestamp()}
                    """.trimMargin())
                } else {
                    logger.error("Error thrown!", e)
                }
            }
        }

        producer.flush()
        producer.close()
    }
}