package com.example.paymentservice.consumer;

import com.example.paymentservice.event.OrderCreatedEvent;
import com.example.paymentservice.service.PaymentService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Kafka consumer that listens to order-events topic
 * 
 * @KafkaListener automatically:
 * - Connects to Kafka broker
 * - Subscribes to topic
 * - Polls for messages
 * - Deserializes JSON → OrderCreatedEvent
 * - Calls this method for each message
 * - Commits offset after successful processing
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class OrderEventConsumer {
    
    private final PaymentService paymentService;
    
    /**
     * Consume OrderCreatedEvent from order-service
     * 
     * @param event Deserialized order event
     */
    @KafkaListener(
        topics = "order-events",
        groupId = "payment-service-group",
        containerFactory = "kafkaListenerContainerFactory"
    )
    public void consumeOrderEvent(OrderCreatedEvent event) {
        log.info("Received OrderCreatedEvent: orderId={}, customerId={}, amount=${}", 
            event.getOrderId(), 
            event.getCustomerId(), 
            event.getTotalAmount());
        
        try {
            // Process payment validation
            paymentService.processOrderPayment(event);
            
            log.info("Successfully processed order: {}", event.getOrderId());
            
        } catch (Exception e) {
            log.error("Failed to process order: {}", event.getOrderId(), e);
            
            // In production:
            // - Retry with exponential backoff
            // - Send to DLQ after max retries
            // - Alert operations team
            throw e;  // Causes Kafka to NOT commit offset (message will be retried)
        }
    }
}