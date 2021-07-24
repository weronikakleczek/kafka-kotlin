package com.example.kafkaconsumer.random

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.kafka.support.KafkaHeaders
import org.springframework.messaging.handler.annotation.Header
import org.springframework.messaging.handler.annotation.Payload
import org.springframework.stereotype.Component
import java.net.InetAddress

@Component
class RandomNumberConsumer {

    val logger: Logger = LoggerFactory.getLogger(RandomNumberConsumer::class.java)
    val consumerName = System.getenv("consumer")

    @KafkaListener(topics = ["animals", "books", "movies", "albums"])
    fun consume(@Payload value: String,
                @Header(KafkaHeaders.RECEIVED_MESSAGE_KEY) key: String,
                @Header(KafkaHeaders.RECEIVED_TOPIC) topic: String,
                @Header(KafkaHeaders.RECEIVED_PARTITION_ID) partition: String) {
        logger.info("[$consumerName]Message $value from topic $topic consumed from producer $key on partition $partition")
    }

}