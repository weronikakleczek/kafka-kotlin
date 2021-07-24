package com.example.kafkaproducer.random

import org.apache.kafka.clients.producer.ProducerRecord
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component
import java.util.concurrent.ThreadLocalRandom

@Component
class RandomNumberProducer(private val kafkaTemplate: KafkaTemplate<String, String>) {

    private val logger: Logger = LoggerFactory.getLogger(RandomNumberProducer::class.java)
    private val producerName = System.getenv("producer")

    @Scheduled(fixedRate = 5000)
    fun produce() {

        when (ThreadLocalRandom.current().nextInt(0, 4)) {
            0 -> produceOnTopicWithMessage("animals", "dog")
            1 -> produceOnTopicWithMessage("books", "1984")
            2 -> produceOnTopicWithMessage("movies", "gattaca")
            3 -> produceOnTopicWithMessage("albums", "in rainbows")
        }

    }

    fun produceOnTopicWithMessage(topic: String, message: String) {
        logger.info("$producerName on topic [$topic] produced value $message")
        this.kafkaTemplate.send(ProducerRecord(topic, producerName, message))
    }

}