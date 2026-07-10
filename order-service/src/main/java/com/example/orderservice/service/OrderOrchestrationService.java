package com.example.orderservice.service;

import com.example.orderservice.event.FinalDecisionEvent;
import com.example.orderservice.event.PaymentProcessedEvent;
import com.example.orderservice.event.StockProcessedEvent;
import com.example.orderservice.model.Order;
import com.example.orderservice.model.OrderStatus;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.Map;
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
     * Track payment and stock responses for 3-service orchestration
     * Map<orderId, Map<"payment"|"stock", response>>
     */
    private final Map<String, Map<String, Object>> orchestrationState = new ConcurrentHashMap<>();

    /**
     * Handle payment response and make final SAGA decision
     *
     * DECISION LOGIC (3-service pattern):
     * - Store payment response and wait for stock response
     * - Both ACCEPT - Order CONFIRMED
     * - Either REJECT - Order REJECTED
     */
    public void handlePaymentResponse(PaymentProcessedEvent paymentEvent) {
        String orderId = paymentEvent.getOrderId();

        log.info("Received payment result for order: {}, status: {}", orderId, paymentEvent.getStatus());

        // Store payment response in orchestration state
        orchestrationState.computeIfAbsent(orderId, k -> new ConcurrentHashMap<>())
            .put("payment", paymentEvent);

        // Try to make final decision if both responses are available
        evaluateOrchestration(orderId);
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

    /**
     * Handle stock response from stock-service (3-service orchestration)
     * Stores the stock response and evaluates if both payment and stock responses are received.
     */
    public void handleStockResult(StockProcessedEvent stockEvent) {
        String orderId = stockEvent.getOrderId();

        log.info("Received stock result for order: {}, status: {}", orderId, stockEvent.getStatus());

        // Store stock response in orchestration state
        orchestrationState.computeIfAbsent(orderId, k -> new ConcurrentHashMap<>())
            .put("stock", stockEvent);

        // Try to make final decision if both responses are available
        evaluateOrchestration(orderId);
    }

    /**
     * Evaluate final decision when both payment and stock responses are available.
     *
     * Decision Logic:
     * - Both ACCEPT -> CONFIRMED
     * - Either REJECT -> REJECTED (compensate the accepted service)
     */
    private void evaluateOrchestration(String orderId) {
        Map<String, Object> state = orchestrationState.get(orderId);
        if (state == null) {
            return;
        }

        PaymentProcessedEvent paymentEvent = (PaymentProcessedEvent) state.get("payment");
        StockProcessedEvent stockEvent = (StockProcessedEvent) state.get("stock");

        // Wait until both responses are received
        if (paymentEvent == null || stockEvent == null) {
            log.info("Waiting for all responses for order: {} (payment: {}, stock: {})",
                orderId, paymentEvent != null, stockEvent != null);
            return;
        }

        // Idempotency check
        if (processedDecisions.contains(orderId)) {
            log.warn("Order {} already processed, skipping", orderId);
            return;
        }

        // Get order
        Order order = orderService.getOrderById(orderId);

        // Make final decision
        FinalDecisionEvent decision;
        boolean paymentAccepted = paymentEvent.getStatus() == PaymentProcessedEvent.PaymentStatus.ACCEPT;
        boolean stockAccepted = stockEvent.getStatus() == StockProcessedEvent.StockStatus.ACCEPT;

        if (paymentAccepted && stockAccepted) {
            // Both accepted - confirm order
            order.setStatus(OrderStatus.CONFIRMED);
            decision = buildDecision(order, FinalDecisionEvent.DecisionStatus.CONFIRMED, null);
            log.info("Both services ACCEPTED - Order CONFIRMED: {}", orderId);
        } else {
            // At least one rejected - reject order
            order.setStatus(OrderStatus.REJECTED);
            String reason = !paymentAccepted ? paymentEvent.getReason() : stockEvent.getReason();
            decision = buildDecision(order, FinalDecisionEvent.DecisionStatus.REJECTED, reason);
            log.info("At least one service REJECTED - Order REJECTED: {} | Reason: {}", orderId, reason);

            // TODO: Trigger compensating transactions if needed
            // e.g., if payment accepted but stock rejected, refund payment
        }

        // Mark as processed and clean up state
        processedDecisions.add(orderId);
        orchestrationState.remove(orderId);

        // Publish final decision
        publishDecision(decision);

        log.info("3-service orchestration complete: orderId={}, status={}", orderId, order.getStatus());
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
