package com.example.orderservice.service;

import com.example.orderservice.event.FinalDecisionEvent;
import com.example.orderservice.event.PaymentProcessedEvent;
import com.example.orderservice.model.Order;
import com.example.orderservice.model.OrderStatus;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * SAGA Orchestration Service
 *
 * Coordinates the distributed transaction between order-service and payment-service.
 *
 * PATTERN: Simple 2-service orchestration (Phase 4)
 * - Consumes PaymentProcessedEvent from payment-service
 * - Makes final decision (CONFIRMED or REJECTED)
 * - Publishes FinalDecisionEvent back to payment-service
 *
 * NOTE: Phase 5 will migrate to Kafka Streams for 3-service orchestration
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class OrderOrchestrationService {

    private static final String ORDER_EVENTS_TOPIC = "order-events";

    private final OrderService orderService;
    private final KafkaTemplate<String, FinalDecisionEvent> kafkaTemplate;

    /**
     * Idempotency tracking - prevents duplicate processing
     */
    private final Set<String> processedDecisions = ConcurrentHashMap.newKeySet();

    /**
     * Handle payment response and make final SAGA decision
     *
     * DECISION LOGIC (2-service pattern):
     * - Payment ACCEPT - Order CONFIRMED
     * - Payment REJECT - Order REJECTED
     *
     * Note: Phase 5 (3-service) will use:
     * - Both ACCEPT - CONFIRMED
     * - Both REJECT - REJECTED
     * - One ACCEPT, one REJECT - ROLLBACK
     */
    public void handlePaymentResponse(PaymentProcessedEvent paymentEvent) {
        String orderId = paymentEvent.getOrderId();

        log.info("Orchestrating decision for order: {}", orderId);

        // Idempotency check
        if (processedDecisions.contains(orderId)) {
            log.warn("Order {} already processed, skipping", orderId);
            return;
        }

        // Get order from EXISTING OrderService.orderStore
        Order order = orderService.getOrderById(orderId);

        // Make final decision based on payment status
        FinalDecisionEvent decision;
        if (paymentEvent.getStatus() == PaymentProcessedEvent.PaymentStatus.ACCEPT) {
            order.setStatus(OrderStatus.CONFIRMED);
            decision = buildDecision(order, FinalDecisionEvent.DecisionStatus.CONFIRMED, null);
            log.info("Payment ACCEPTED - Order CONFIRMED: {}", orderId);
        } else {
            order.setStatus(OrderStatus.REJECTED);
            decision = buildDecision(order, FinalDecisionEvent.DecisionStatus.REJECTED, paymentEvent.getReason());
            log.info("Payment REJECTED - Order REJECTED: {} | Reason: {}", orderId, paymentEvent.getReason());
        }

        // Mark as processed
        processedDecisions.add(orderId);

        // Publish final decision to order-events topic
        publishDecision(decision);

        log.info("Orchestration complete: orderId={}, status={}", orderId, order.getStatus());
    }

    private FinalDecisionEvent buildDecision(Order order, FinalDecisionEvent.DecisionStatus status, String reason) {
        return FinalDecisionEvent.builder()
            .orderId(order.getOrderId())
            .customerId(order.getCustomerId())
            .amount(order.getTotalAmount())
            .status(status)
            .reason(reason)
            .decidedAt(LocalDateTime.now())
            .build();
    }

    private void publishDecision(FinalDecisionEvent decision) {
        kafkaTemplate.send(ORDER_EVENTS_TOPIC, decision.getOrderId(), decision)
            .whenComplete((result, ex) -> {
                if (ex != null) {
                    log.error("Failed to publish decision: {}", decision.getOrderId(), ex);
                } else {
                    log.info("Published FinalDecisionEvent: orderId={}, status={}, offset={}",
                        decision.getOrderId(), decision.getStatus(),
                        result.getRecordMetadata().offset());
                }
            });
    }
}
