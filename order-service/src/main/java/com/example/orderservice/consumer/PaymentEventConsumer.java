package com.example.orderservice.consumer;

import com.example.orderservice.event.PaymentProcessedEvent;
import com.example.orderservice.service.OrderOrchestrationService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
@Slf4j
public class PaymentEventConsumer {

    private final OrderOrchestrationService orchestrationService;

    @KafkaListener(
        topics = "payment-events",
        groupId = "order-service-group",
        containerFactory = "kafkaListenerContainerFactory"
    )
    public void consumePaymentEvent(PaymentProcessedEvent event) {
        log.info("Received PaymentProcessedEvent: orderId={}, status={}",
            event.getOrderId(), event.getStatus());

        try {
            orchestrationService.handlePaymentResponse(event);
            log.info("Successfully processed payment event for order: {}", event.getOrderId());
        } catch (Exception e) {
            log.error("Failed to process payment event for order: {}", event.getOrderId(), e);
            throw e;
        }
    }
}
