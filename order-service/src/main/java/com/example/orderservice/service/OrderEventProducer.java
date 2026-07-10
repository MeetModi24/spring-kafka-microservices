package com.example.orderservice.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import com.example.orderservice.event.OrderCreatedEvent;

import java.util.concurrent.CompletableFuture;

/**
 * Kafka Producer for Order Events
 *
 * DESIGN PATTERN: Publisher in Pub/Sub pattern
 * - Publishes events to Kafka topics
 * - Other services (payment, stock) subscribe to topics
 * - Asynchronous, non-blocking communication
 *
 * WHY SEPARATE PRODUCER SERVICE?
 * - Single Responsibility Principle: OrderService handles business logic,
 *   OrderEventProducer handles event publishing
 * - Easier to test (can mock producer in OrderService tests)
 * - Reusable for different event types
 * - Can add retry logic, circuit breaker, etc. in one place
 */
@Service
public class OrderEventProducer {

    private static final Logger log = LoggerFactory.getLogger(OrderEventProducer.class);
    private static final String TOPIC = "order-created";

    private final KafkaTemplate<String, OrderCreatedEvent> kafkaTemplate;

    /**
     * Constructor injection of KafkaTemplate
     * Spring Boot auto-configures KafkaTemplate bean based on application.yml
     */
    public OrderEventProducer(KafkaTemplate<String, OrderCreatedEvent> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    /**
     * Publish OrderCreatedEvent to Kafka
     *
     * @param event The event to publish
     * @return CompletableFuture for async result
     *
     * ASYNCHRONOUS PROCESSING:
     * - Method returns immediately (non-blocking)
     * - Kafka send happens in background I/O thread
     * - Callbacks executed when send completes
     *
     * KEY DESIGN:
     * - Key = orderId ensures all events for same order go to same partition
     * - Same partition = ordering guaranteed for that order
     * - Different orders can be processed in parallel across partitions
     */
    public CompletableFuture<SendResult<String, OrderCreatedEvent>> publishOrderCreated(
            OrderCreatedEvent event) {

        log.info("Publishing OrderCreatedEvent: orderId={}", event.getOrderId());

        // Send to Kafka
        // Parameters:
        // 1. topic: "order-events"
        // 2. key: orderId (for partition routing and ordering)
        // 3. value: event object (will be serialized to JSON)
        CompletableFuture<SendResult<String, OrderCreatedEvent>> future =
            kafkaTemplate.send(TOPIC, event.getOrderId(), event);

        // Add success/failure callbacks
        // whenComplete runs after send completes (success or failure)
        future.whenComplete((result, ex) -> {
            if (ex == null) {
                // Success case
                log.info("Message sent successfully: orderId={}, offset={}, partition={}",
                    event.getOrderId(),
                    result.getRecordMetadata().offset(),
                    result.getRecordMetadata().partition());
            } else {
                // Failure case
                log.error("Failed to send message: orderId={}, error={}",
                    event.getOrderId(),
                    ex.getMessage(),
                    ex);

                // In production, you might:
                // - Store failed events in database for retry
                // - Send to dead-letter queue
                // - Trigger alert/notification
            }
        });

        return future;
    }

    /**
     * Synchronous version (blocks until complete)
     * Use this only if you need confirmation before proceeding
     *
     * WARNING: Blocks the thread, reduces throughput
     */
    public SendResult<String, OrderCreatedEvent> publishOrderCreatedSync(
            OrderCreatedEvent event) throws Exception {

        log.info("Publishing OrderCreatedEvent (sync): orderId={}", event.getOrderId());

        // .get() blocks until future completes
        return kafkaTemplate.send(TOPIC, event.getOrderId(), event).get();
    }
}
