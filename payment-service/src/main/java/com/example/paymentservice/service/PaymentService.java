package com.example.paymentservice.service;

import com.example.paymentservice.event.FinalDecisionEvent;
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
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Payment validation business logic
 *
 * SAGA PATTERN: Participant Service
 * Phase 1 (Reserve): Receives order event → Validates payment → Publishes response
 * Phase 2 (Confirm/Rollback): Receives final decision → Commits or compensates
 *
 * TWO-PHASE COMMIT:
 * 1. Reserve: Tentatively lock funds (amountAvailable → amountReserved)
 * 2a. Confirm: Permanently deduct from reserved (amountReserved → 0)
 * 2b. Rollback: Return to available (amountReserved → amountAvailable)
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class PaymentService {

    private static final String PAYMENT_EVENTS_TOPIC = "payment-events";

    private final CustomerRepository customerRepository;
    private final KafkaTemplate<String, PaymentProcessedEvent> kafkaTemplate;

    /**
     * Idempotency tracking for reservations
     * Prevents duplicate processing of OrderCreatedEvent
     *
     * PRODUCTION: Use Redis or database table with TTL
     */
    private final Set<String> processedReservations = ConcurrentHashMap.newKeySet();

    /**
     * Idempotency tracking for final decisions
     * Prevents duplicate processing of FinalDecisionEvent
     *
     * KEY BENEFIT: If Kafka retries decision message, we don't double-deduct
     *
     * PRODUCTION: Use Redis or database table with TTL
     * Example: SET processed:decision:{orderId} "true" EX 86400
     */
    private final Set<String> processedDecisions = ConcurrentHashMap.newKeySet();
    
    /**
     * Process order payment validation (SAGA Reserve phase)
     *
     * IDEMPOTENCY: Tracks processed order IDs to prevent duplicate reservations
     *
     * @param event OrderCreatedEvent from order-service
     */
    @Transactional
    public void processOrderPayment(OrderCreatedEvent event) {
        log.info("Processing payment for order: {}", event.getOrderId());

        // Idempotency check: Skip if already processed
        if (processedReservations.contains(event.getOrderId())) {
            log.warn("Order already processed (duplicate message): {}", event.getOrderId());
            return;
        }

        // Find customer
        Optional<Customer> customerOpt = customerRepository.findByCustomerId(event.getCustomerId());

        if (customerOpt.isEmpty()) {
            log.warn("Customer not found: {}", event.getCustomerId());
            publishRejection(event, "Customer not found");
            processedReservations.add(event.getOrderId());
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

        // Mark as processed
        processedReservations.add(event.getOrderId());
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
            .build()
;
        kafkaTemplate.send(PAYMENT_EVENTS_TOPIC, event.getOrderId(), paymentEvent);
        log.info("Published PaymentProcessedEvent: REJECT for order {} | Reason: {}",
            event.getOrderId(), reason);
    }

    /**
     * Handle order confirmation (SAGA Confirm phase)
     *
     * Called when order-service publishes FinalDecisionEvent with status=CONFIRMED
     *
     * BUSINESS LOGIC:
     * - Moves funds from reserved → permanently deducted
     * - Customer.amountReserved decreases
     * - No refund to amountAvailable
     *
     * IDEMPOTENCY:
     * - Uses processedDecisions set to prevent duplicate confirm
     * - Critical for exactly-once semantics in distributed systems
     *
     * ERROR HANDLING:
     * - If customer not found: Log error but don't throw (order already completed)
     * - If insufficient reserved: Log warning (data inconsistency - investigate!)
     *
     * @param event FinalDecisionEvent from order-service
     */
    @Transactional
    public void handleConfirm(FinalDecisionEvent event) {
        log.info("Handling CONFIRM decision for order: {}", event.getOrderId());

        // Idempotency check: Skip if already processed
        if (processedDecisions.contains(event.getOrderId())) {
            log.warn("Decision already processed (duplicate message): {}", event.getOrderId());
            return;
        }

        // Find customer
        Optional<Customer> customerOpt = customerRepository.findByCustomerId(event.getCustomerId());

        if (customerOpt.isEmpty()) {
            log.error("Customer not found during confirm: {} | Order: {}",
                event.getCustomerId(), event.getOrderId());
            processedDecisions.add(event.getOrderId());
            return;
        }

        Customer customer = customerOpt.get();

        // We need to get the original amount from somewhere
        // In real system: query database for reservation amount
        // For now: log that we're confirming whatever was reserved
        log.info("Confirming payment for order: {} | Customer: {} | Reserved balance: ${}",
            event.getOrderId(),
            customer.getCustomerId(),
            customer.getAmountReserved() / 100.0);

        // NOTE: In production, store reservation details in database:
        // CREATE TABLE payment_reservations (
        //   order_id VARCHAR PRIMARY KEY,
        //   customer_id VARCHAR,
        //   amount_cents INT,
        //   reserved_at TIMESTAMP
        // )
        //
        // Then: SELECT amount_cents FROM payment_reservations WHERE order_id = ?

        // For demo: Confirm all reserved funds for this customer
        // In production: Confirm specific amount for this order
        Integer reservedAmount = customer.getAmountReserved();
        if (reservedAmount > 0) {
            customer.confirm(reservedAmount);
            customerRepository.save(customer);

            log.info("Payment CONFIRMED for order: {} | Deducted: ${}",
                event.getOrderId(), reservedAmount / 100.0);
        } else {
            log.warn("No reserved amount to confirm for order: {} | Possible data inconsistency",
                event.getOrderId());
        }

        // Mark as processed
        processedDecisions.add(event.getOrderId());
    }

    /**
     * Handle order rollback (SAGA Compensate phase)
     *
     * Called when order-service publishes FinalDecisionEvent with status=REJECTED
     *
     * BUSINESS LOGIC:
     * - Returns funds from reserved → available
     * - Customer.amountReserved decreases
     * - Customer.amountAvailable increases (refund)
     *
     * WHEN THIS HAPPENS:
     * - Another participant (inventory-service) rejected the order
     * - Order-service decided to abort the transaction
     * - All participants must compensate their tentative changes
     *
     * IDEMPOTENCY:
     * - Uses processedDecisions set to prevent duplicate rollback
     * - Prevents double-refund if Kafka retries message
     *
     * ERROR HANDLING:
     * - If customer not found: Log error but don't throw (best effort)
     * - If insufficient reserved: Log warning (data inconsistency)
     *
     * @param event FinalDecisionEvent from order-service
     */
    @Transactional
    public void handleRollback(FinalDecisionEvent event) {
        log.info("Handling ROLLBACK decision for order: {} | Reason: {}",
            event.getOrderId(), event.getReason());

        // Idempotency check: Skip if already processed
        if (processedDecisions.contains(event.getOrderId())) {
            log.warn("Decision already processed (duplicate message): {}", event.getOrderId());
            return;
        }

        // Find customer
        Optional<Customer> customerOpt = customerRepository.findByCustomerId(event.getCustomerId());

        if (customerOpt.isEmpty()) {
            log.error("Customer not found during rollback: {} | Order: {}",
                event.getCustomerId(), event.getOrderId());
            processedDecisions.add(event.getOrderId());
            return;
        }

        Customer customer = customerOpt.get();

        log.info("Rolling back payment for order: {} | Customer: {} | Reserved balance: ${}",
            event.getOrderId(),
            customer.getCustomerId(),
            customer.getAmountReserved() / 100.0);

        // Rollback reserved funds to available
        Integer reservedAmount = customer.getAmountReserved();
        if (reservedAmount > 0) {
            customer.rollback(reservedAmount);
            customerRepository.save(customer);

            log.info("Payment ROLLED BACK for order: {} | Returned to available: ${}",
                event.getOrderId(), reservedAmount / 100.0);
        } else {
            log.warn("No reserved amount to rollback for order: {} | Possible data inconsistency",
                event.getOrderId());
        }

        // Mark as processed
        processedDecisions.add(event.getOrderId());
    }
}