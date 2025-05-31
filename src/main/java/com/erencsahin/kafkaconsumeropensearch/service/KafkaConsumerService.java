package com.erencsahin.kafkaconsumeropensearch.service;

import com.erencsahin.kafkaconsumeropensearch.dto.Rate;
import lombok.RequiredArgsConstructor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class KafkaConsumerService {
    private static final Logger logger = LogManager.getLogger(KafkaConsumerService.class);
    private final OpenSearchService openSearchService;

    @KafkaListener(topics = "avg-data", groupId = "os-consumers", containerFactory = "kafkaListenerContainerFactory")
    public void consume(@Payload Rate rate,
                        @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
                        @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
                        @Header(KafkaHeaders.OFFSET) long offset,
                        Acknowledgment acknowledgment) {

        try {
            logger.info("Kafka mesajı alındı - Topic: {}, Offset: {}, Rate: {}", topic, offset, rate);
            openSearchService.indexRate(rate);
            acknowledgment.acknowledge();
        } catch (Exception e) {
            logger.error("Hata oluştu - Rate: {}", rate, e);
        }
    }
}
