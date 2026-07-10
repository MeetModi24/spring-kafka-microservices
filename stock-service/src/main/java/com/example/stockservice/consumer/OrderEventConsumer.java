package com.example.stockservice.consumer;

import com.example.stockservice.event.OrderCreatedEvent;
import com.example.stockservice.event.FinalDecisionEvent;
import com.example.stockservice.event.StockProcessedEvent;
import com.example.stockservice.service.StockService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import static com.example.stockservice.event.FinalDecisionEvent.DecisionStatus.*;

@Component
@Slf4j
@RequiredArgsConstructor
public class OrderEventConsumer {

    private final StockService stockService;
    private final KafkaTemplate<String, Object> kafkaTemplate;

    private static final String STOCK_EVENTS_TOPIC = "stock-events";

    /**
     * Consumer 1: Listen for new orders (reserve stock for ALL items).
     */
    @KafkaListener(topics = "order-created", groupId = "stock-service-group")
    public void consumeOrderEvent(@Payload OrderCreatedEvent event,
                                  @Header(KafkaHeaders.RECEIVED_KEY) String key) {

        log.info("Received OrderCreatedEvent: orderId={}, customerId={}, items={}",
                 event.getOrderId(), event.getCustomerId(), event.getItems().size());

        // Log all items for visibility
        event.getItems().forEach(item ->
            log.info("  - Product: {} | Quantity: {} | Price: {}",
                     item.getProductId(), item.getQuantity(), item.getPrice()));

        // Process ALL items atomically
        StockProcessedEvent stockEvent = stockService.processOrderStock(event);

        // Publish stock response
        kafkaTemplate.send(STOCK_EVENTS_TOPIC, event.getOrderId(), stockEvent);
        log.info("Published StockProcessedEvent: orderId={}, status={}, items={}",
                 event.getOrderId(), stockEvent.getStatus(), stockEvent.getItems().size());
    }

    /**
     * Consumer 2: Listen for final decisions (confirm/rollback).
     */
    @KafkaListener(topics = "order-events", groupId = "stock-decision-group")
    public void consumeDecisionEvent(@Payload FinalDecisionEvent event,
                                     @Header(KafkaHeaders.RECEIVED_KEY) String key) {

        log.info("Received FinalDecisionEvent: orderId={}, status={}, source={}",
                 event.getOrderId(), event.getStatus(), event.getSource());

        if (event.getStatus() == CONFIRMED) {
            stockService.handleConfirm(event.getOrderId());
        } else if (event.getStatus() == ROLLBACK) {
            // Only rollback if stock was the successful service
            // (payment failed, so we need to return reserved stock)
            if (!"STOCK".equals(event.getSource())) {
                log.info("ROLLBACK triggered by payment failure - returning stock to inventory");
                stockService.handleRollback(event.getOrderId());
            } else {
                log.info("ROLLBACK triggered by stock failure - no compensation needed");
            }
        } else if (event.getStatus() == REJECTED) {
            // Both services rejected - nothing to rollback
            log.info("Order REJECTED - no stock compensation needed");
        }
    }
}
