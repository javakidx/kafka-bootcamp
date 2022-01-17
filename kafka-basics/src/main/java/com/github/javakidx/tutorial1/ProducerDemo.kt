package com.github.javakidx.tutorial1

import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer
import java.util.*

object ProducerDemo {
    @JvmStatic
    fun main(args: Array<String>) {
        val topic = "first_topic"
        val bootstrapServers = "127.0.0.1:9092"
        //print(bootstrapServers);
        val properties = Properties()
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java.name)
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer::class.java.name)
        //properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        print(properties)

        val producer: KafkaProducer<String, String> = KafkaProducer(properties)
        val message: ProducerRecord<String, String> = ProducerRecord(topic, "Hello world ya!")

        producer.send(message)

        producer.flush()
        producer.close()
    }
}