package com.example.paymentservice.service;

import com.example.paymentservice.event.OrderCreatedEvent;
import com.example.paymentservice.event.PaymentProcessedEvent;
import com.example.paymentservice.model.Customer;
import com.example.paymentservice.repository.CustomerRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.Optional;

/**
 * Payment validation business logic
 * 
 * SAGA PATTERN: Participant Service
 * - Receives order event
 * - Validates payment (reserve funds)
 * - Publishes response (ACCEPT/REJECT)
 * - Later receives final decision (confirm/rollback)
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class PaymentService {
    
    private static final String PAYMENT_EVENTS_TOPIC = "payment-events";
    
    private final CustomerRepository customerRepository;
    private final KafkaTemplate<String, PaymentProcessedEvent> kafkaTemplate;
    
    /**
     * Process order payment validation (SAGA Reserve phase)
     * 
     * @param event OrderCreatedEvent from order-service
     */
    @Transactional
    public void processOrderPayment(OrderCreatedEvent event) {
        log.info("Processing payment for order: {}", event.getOrderId());
        
        // Find customer
        Optional<Customer> customerOpt = customerRepository.findByCustomerId(event.getCustomerId());
        
        if (customerOpt.isEmpty()) {
            log.warn("Customer not found: {}", event.getCustomerId());
            publishRejection(event, "Customer not found");
            return;
        }
        
        Customer customer = customerOpt.get();
        Integer amountCents = event.getTotalAmount().multiply(new java.math.BigDecimal("100")).intValue();
        
        log.info("Customer {} | Available: ${} | Required: ${}", 
            customer.getCustomerId(), 
            customer.getAmountAvailable() / 100.0,
            amountCents / 100.0);
        
        // Attempt to reserve funds
        boolean reserved = customer.reserve(amountCents);
        
        if (reserved) {
            // Save updated balance
            customerRepository.save(customer);
            
            log.info("Payment ACCEPTED for order: {} | Reserved: ${}", 
                event.getOrderId(), amountCents / 100.0);
            
            publishAcceptance(event, amountCents);
        } else {
            log.warn("Payment REJECTED for order: {} | Insufficient balance", 
                event.getOrderId());
            
            publishRejection(event, "Insufficient balance");
        }
    }
    
    /**
     * Publish payment acceptance event
     */
    private void publishAcceptance(OrderCreatedEvent event, Integer amountCents) {
        PaymentProcessedEvent paymentEvent = PaymentProcessedEvent.builder()
            .orderId(event.getOrderId())
            .customerId(event.getCustomerId())
            .amount(event.getTotalAmount())
            .status(PaymentProcessedEvent.PaymentStatus.ACCEPT)
            .processedAt(LocalDateTime.now())
            .build();
        
        kafkaTemplate.send(PAYMENT_EVENTS_TOPIC, event.getOrderId(), paymentEvent);
        log.info("Published PaymentProcessedEvent: ACCEPT for order {}", event.getOrderId());
    }
    
    /**
     * Publish payment rejection event
     */
    private void publishRejection(OrderCreatedEvent event, String reason) {
        PaymentProcessedEvent paymentEvent = PaymentProcessedEvent.builder()
            .orderId(event.getOrderId())
            .customerId(event.getCustomerId())
            .amount(event.getTotalAmount())
            .status(PaymentProcessedEvent.PaymentStatus.REJECT)
            .reason(reason)
            .processedAt(LocalDateTime.now())
            .build();
        
        kafkaTemplate.send(PAYMENT_EVENTS_TOPIC, event.getOrderId(), paymentEvent);
        log.info("Published PaymentProcessedEvent: REJECT for order {} | Reason: {}", 
            event.getOrderId(), reason);
    }
}