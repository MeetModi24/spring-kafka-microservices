package com.example.orderservice.consumer;

import com.example.orderservice.event.StockProcessedEvent;
import com.example.orderservice.service.OrderOrchestrationService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

/**
 * Consumes StockProcessedEvent from stock-service.
 * Part of the orchestration layer - receives stock processing results.
 */
@Component
@Slf4j
@RequiredArgsConstructor
public class StockEventConsumer {

    private final OrderOrchestrationService orchestrationService;

    /**
     * Listens to stock-events topic for stock processing results.
     * When received, delegates to orchestration service to make final decision.
     *
     * @param event StockProcessedEvent from stock-service
     */
    @KafkaListener(
        topics = "stock-events",
        groupId = "order-service-stock-group",
        containerFactory = "kafkaListenerContainerFactory"
    )
    public void consumeStockEvent(@Payload StockProcessedEvent event) {
        log.info("Received StockProcessedEvent: orderId={}, status={}, items={}",
                 event.getOrderId(),
                 event.getStatus(),
                 event.getItems() != null ? event.getItems().size() : 0);

        try {
            // Delegate to orchestration service
            orchestrationService.handleStockResult(event);

            log.info("Successfully processed StockProcessedEvent for order: {}", event.getOrderId());
        } catch (Exception e) {
            log.error("Error processing StockProcessedEvent for order: {} | Error: {}",
                     event.getOrderId(), e.getMessage(), e);
            // In production: send to DLQ or retry queue
        }
    }
}
