package com.example.orderservice.consumer;

import com.example.orderservice.event.FinalDecisionEvent;
import com.example.orderservice.model.Order;
import com.example.orderservice.model.OrderStatus;
import com.example.orderservice.service.OrderService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Consumes final SAGA decisions from Kafka Streams and updates order status.
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class DecisionEventConsumer {

    private final OrderService orderService;

    @KafkaListener(
        topics = "order-events",
        groupId = "order-decision-group",
        containerFactory = "kafkaListenerContainerFactory"
    )
    public void consumeDecision(FinalDecisionEvent decision) {
        log.info("Received FinalDecisionEvent: orderId={}, status={}",
                 decision.getOrderId(), decision.getStatus());

        try {
            Order order = orderService.getOrderById(decision.getOrderId());

            // Map decision status to order status
            OrderStatus newStatus = switch (decision.getStatus()) {
                case CONFIRMED -> OrderStatus.CONFIRMED;
                case REJECTED -> OrderStatus.REJECTED;
                case ROLLBACK -> OrderStatus.ROLLBACK;
            };

            order.setStatus(newStatus);
            orderService.updateOrder(order);

            log.info("Order {} status updated to {}", decision.getOrderId(), newStatus);
        } catch (Exception e) {
            log.error("Failed to process decision for order: {}", decision.getOrderId(), e);
        }
    }
}
