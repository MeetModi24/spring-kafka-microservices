package com.example.paymentservice.consumer;

import com.example.paymentservice.event.FinalDecisionEvent;
import com.example.paymentservice.service.PaymentService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Kafka consumer that listens to final decision events from order-service
 *
 * KEY PATTERN: Two listeners on same topic with different consumer groups
 *
 * SAME TOPIC, DIFFERENT CONSUMER GROUPS:
 * - OrderEventConsumer: groupId="payment-service-group" → handles OrderCreatedEvent
 * - DecisionEventConsumer: groupId="payment-decision-group" → handles FinalDecisionEvent
 *
 * HOW KAFKA ROUTES MESSAGES:
 * 1. Both event types published to "order-events" topic
 * 2. Kafka maintains SEPARATE offsets for each consumer group
 * 3. Each consumer group receives ALL messages independently
 * 4. OrderEventConsumer filters for OrderCreatedEvent (via type mapping)
 * 5. DecisionEventConsumer filters for FinalDecisionEvent (via type mapping)
 *
 * WHY THIS PATTERN:
 * - Simpler than status-based filtering in single listener
 * - Type-safe deserialization (compiler-checked)
 * - Independent scaling (can scale each consumer group separately)
 * - Clear separation of concerns (reserve vs confirm/rollback)
 *
 * ALTERNATIVE PATTERN (from reference repo):
 * ```java
 * @KafkaListener(id = "orders", topics = "orders", groupId = "payment")
 * public void onEvent(Order order) {
 *     if (order.getStatus().equals("NEW")) {
 *         orderManageService.reserve(order);
 *     } else if (order.getStatus().equals("CONFIRMED")) {
 *         orderManageService.confirm(order);
 *     }
 * }
 * ```
 * Our pattern is more maintainable with separate DTOs for each event type.
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class DecisionEventConsumer {

    private final PaymentService paymentService;

    /**
     * Consume FinalDecisionEvent from order-service
     *
     * @KafkaListener configuration:
     * - topics: Same as OrderEventConsumer ("order-events")
     * - groupId: DIFFERENT from OrderEventConsumer ("payment-decision-group")
     * - containerFactory: Reuses same JSON deserializer factory
     *
     * Type Mapping:
     * - Spring deserializes to FinalDecisionEvent via type header
     * - Type mapping configured in application.yml
     *
     * @param event Deserialized final decision event
     */
    @KafkaListener(
        topics = "order-events",
        groupId = "payment-decision-group",
        containerFactory = "kafkaListenerContainerFactory"
    )
    public void consumeDecisionEvent(FinalDecisionEvent event) {
        log.info("Received FinalDecisionEvent: orderId={}, status={}, customerId={}",
            event.getOrderId(),
            event.getStatus(),
            event.getCustomerId());

        try {
            // Route to confirm or rollback based on decision status
            if (event.getStatus() == FinalDecisionEvent.DecisionStatus.CONFIRMED) {
                log.info("Order CONFIRMED by orchestrator - committing reservation for order: {}",
                    event.getOrderId());
                paymentService.handleConfirm(event);

            } else if (event.getStatus() == FinalDecisionEvent.DecisionStatus.REJECTED) {
                log.info("Order REJECTED by orchestrator - rolling back reservation for order: {}",
                    event.getOrderId());
                paymentService.handleRollback(event);

            } else {
                log.warn("Unknown decision status: {} for order: {}",
                    event.getStatus(), event.getOrderId());
            }

            log.info("Successfully processed final decision for order: {}", event.getOrderId());

        } catch (Exception e) {
            log.error("Failed to process final decision for order: {}", event.getOrderId(), e);

            // PRODUCTION ERROR HANDLING:
            // 1. Retry with exponential backoff (configure in application.yml)
            // 2. Send to Dead Letter Queue (DLQ) after max retries
            // 3. Alert operations team
            // 4. Log to centralized logging (Splunk, ELK, etc.)
            //
            // Throwing exception prevents Kafka offset commit
            // Message will be retried according to retry policy
            throw e;
        }
    }
}
