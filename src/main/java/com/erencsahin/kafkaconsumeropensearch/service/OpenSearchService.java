package com.erencsahin.kafkaconsumeropensearch.service;

import com.erencsahin.kafkaconsumeropensearch.dto.Rate;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestHighLevelClient;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.time.Instant;
import java.util.Map;

@Service
public class OpenSearchService {
    private static final Logger logger = LogManager.getLogger(OpenSearchService.class);
    private final RestHighLevelClient client;

    public OpenSearchService(RestHighLevelClient client) {
        this.client = client;
        logger.info("OpenSearchService başlatıldı.");
    }

    public void indexRate(Rate rate) {
        Map<String, Object> rateMap = Map.of(
                "symbol", rate.getSymbol(),
                "ask", rate.getAsk(),
                "bid", rate.getBid(),
                "rateTime", rate.getTimestamp(),
                "openSearchInsertedTime", Instant.now().toString()
        );

        String docId = rate.getSymbol() + ":" + rate.getTimestamp();
        IndexRequest request = new IndexRequest("rates").id(docId).source(rateMap);

        try {
            client.index(request, RequestOptions.DEFAULT);
            logger.info("OpenSearch'e veri yazıldı - ID: {}", docId);
        } catch (IOException e) {
            logger.error("OpenSearch indexleme hatası - ID: {}", docId, e);
            throw new RuntimeException("OpenSearch indexleme hatası", e);
        }
    }
}
