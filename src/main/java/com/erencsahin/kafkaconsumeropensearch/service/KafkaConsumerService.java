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

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

@Service
@RequiredArgsConstructor
public class KafkaConsumerService {
    private static final Logger logger = LogManager.getLogger(KafkaConsumerService.class);
    private final OpenSearchService openSearchService;

    private static final Set<String> SYMBOLS = Set.of("USDTRY", "EURTRY", "GBPTRY");
    private final Map<String, Rate> lastValidRateMap = new ConcurrentHashMap<>();

    /** %1 threshold for outlier detection */
    private static final double MAX_DIFF_PERCENTAGE = 0.01; // %1 = 0.01

    /** Maximum consecutive outliers before forcing acceptance */
    private final Map<String, Integer> consecutiveOutlierCount = new ConcurrentHashMap<>();
    private static final int MAX_CONSECUTIVE_OUTLIERS = 5;

    @KafkaListener(topics = "avg-data", groupId = "os-consumers", containerFactory = "kafkaListenerContainerFactory")
    public void consume(@Payload Rate rate,
                        @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
                        @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
                        @Header(KafkaHeaders.OFFSET) long offset,
                        Acknowledgment acknowledgment) {

        String symbol = rate.getSymbol();

        // 1) İzinli çift mi?
        if (!SYMBOLS.contains(symbol)) {
            logger.warn("Unknown symbol '{}' has been skipped.", symbol);
            if (acknowledgment != null) acknowledgment.acknowledge();
            return;
        }

        try {
            logger.info("Kafka mesajı alındı - Topic: {}, Partition: {}, Offset: {}, Rate: {}",
                    topic, partition, offset, rate);

            // 2) Filtreleme ve değer belirleme
            Rate rateToIndex = applyOutlierFilter(rate, symbol);

            // 3) Sadece null değilse OpenSearch'e kaydet
            if (rateToIndex != null) {
                openSearchService.indexRate(rateToIndex);
                logger.info("Data indexed to OpenSearch - symbol: {}, ask: {}, bid: {}",
                        symbol, rateToIndex.getAsk(), rateToIndex.getBid());
            } else {
                logger.info("Outlier detected - skipping OpenSearch indexing for symbol: {}", symbol);
            }

            // 4) Acknowledge
            if (acknowledgment != null) {
                acknowledgment.acknowledge();
                logger.debug("Acknowledged offset {}", offset);
            }

        } catch (Exception e) {
            logger.error("Hata oluştu - Rate: {}, Error: {}", rate, e.getMessage(), e);
            throw e; // retry
        }
    }

    private Rate applyOutlierFilter(Rate newRate, String symbol) {
        double newAsk = newRate.getAsk();
        double newBid = newRate.getBid();
        double newMid = (newAsk + newBid) / 2.0;

        Rate lastValidRate = lastValidRateMap.get(symbol);

        // İlk kayıt durumu
        if (lastValidRate == null) {
            lastValidRateMap.put(symbol, newRate);
            consecutiveOutlierCount.put(symbol, 0);
            logger.debug("First record for {}: {}", symbol, newMid);
            return newRate;
        }

        // Önceki geçerli değerlerle karşılaştır
        double lastValidAsk = lastValidRate.getAsk();
        double lastValidBid = lastValidRate.getBid();
        double lastValidMid = (lastValidAsk + lastValidBid) / 2.0;

        double percentageChange = Math.abs(newMid - lastValidMid) / lastValidMid;
        int currentOutlierCount = consecutiveOutlierCount.getOrDefault(symbol, 0);

        // Outlier kontrolü
        if (percentageChange > MAX_DIFF_PERCENTAGE) {
            // Çok fazla ardışık outlier varsa zorla kabul et
            if (currentOutlierCount >= MAX_CONSECUTIVE_OUTLIERS) {
                logger.warn("Forcing acceptance after {} consecutive outliers for {}: newMid={}, lastMid={}, change={}%",
                        MAX_CONSECUTIVE_OUTLIERS, symbol, newMid, lastValidMid, percentageChange * 100);

                lastValidRateMap.put(symbol, newRate);
                consecutiveOutlierCount.put(symbol, 0);
                return newRate;
            } else {
                // Outlier: kaydetme
                consecutiveOutlierCount.put(symbol, currentOutlierCount + 1);

                logger.warn("Outlier detected for {}: newMid={}, lastMid={}, change={}%. Skipping indexing. (Consecutive: {})",
                        symbol, newMid, lastValidMid, percentageChange * 100, currentOutlierCount + 1);

                return null; // Kaydetme sinyali
            }
        } else {
            // Kabul edilebilir değişim: yeni değeri kullan ve referansı güncelle
            lastValidRateMap.put(symbol, newRate);
            consecutiveOutlierCount.put(symbol, 0);

            logger.debug("Accepted new value for {}: newMid={}, change={}%",
                    symbol, newMid, percentageChange * 100);

            return newRate;
        }
    }
}