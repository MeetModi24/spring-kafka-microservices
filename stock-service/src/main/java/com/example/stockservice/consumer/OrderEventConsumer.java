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

import java.util.List;
import java.util.stream.Collectors;

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
     *
     * ATOMIC RESERVATION:
     * - Passes entire event to service layer
     * - Service handles atomic reservation (all or nothing)
     */
    @KafkaListener(topics = "order-events", groupId = "stock-service-group")
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
     * Consumer 2: Listen for final decisions (confirm/rollback ALL items).
     *
     * COMPENSATION HANDLING:
     * - CONFIRMED: Deduct all reserved items permanently
     * - ROLLBACK: Return all reserved items to available pool
     * - REJECTED: No-op (nothing was reserved)
     */
    @KafkaListener(topics = "order-events", groupId = "stock-decision-group")
    public void consumeDecisionEvent(@Payload FinalDecisionEvent event,
                                     @Header(KafkaHeaders.RECEIVED_KEY) String key) {

        log.info("Received FinalDecisionEvent: orderId={}, status={}, items={}",
                 event.getOrderId(), event.getStatus(), event.getItems().size());

        // Convert FinalDecisionEvent back to OrderCreatedEvent structure for processing
        // (Both have same fields: orderId, customerId, items)
        OrderCreatedEvent orderEvent = convertToOrderCreatedEvent(event);

        if (event.getStatus() == CONFIRMED) {
            stockService.handleConfirm(orderEvent);
        } else if (event.getStatus() == ROLLBACK) {
            // Only rollback if stock was the successful service
            // (payment failed, so we need to return reserved stock)
            if (!"STOCK".equals(event.getSource())) {
                log.info("ROLLBACK triggered by payment failure - returning stock to inventory");
                stockService.handleRollback(orderEvent);
            } else {
                log.info("ROLLBACK triggered by stock failure - no compensation needed");
            }
        } else if (event.getStatus() == REJECTED) {
            // Both services rejected - nothing to rollback
            log.info("Order REJECTED - no stock compensation needed");
        }
    }

    /**
     * Helper to convert FinalDecisionEvent to OrderCreatedEvent structure.
     * Both events have the same core fields (orderId, customerId, items).
     */
    private OrderCreatedEvent convertToOrderCreatedEvent(FinalDecisionEvent event) {
        OrderCreatedEvent orderEvent = new OrderCreatedEvent();
        orderEvent.setOrderId(event.getOrderId());
        orderEvent.setCustomerId(event.getCustomerId());

        // Convert item DTOs
        List<OrderCreatedEvent.OrderItemEvent> items = event.getItems().stream()
            .map(item -> new OrderCreatedEvent.OrderItemEvent(
                item.getProductId(),
                item.getProductName(),
                item.getQuantity(),
                item.getPrice()
            ))
            .collect(Collectors.toList());

        orderEvent.setItems(items);
        orderEvent.setTotalAmount(event.getItems().stream()
            .map(item -> item.getPrice().multiply(java.math.BigDecimal.valueOf(item.getQuantity())))
            .reduce(java.math.BigDecimal.ZERO, java.math.BigDecimal::add));

        return orderEvent;
    }
}
