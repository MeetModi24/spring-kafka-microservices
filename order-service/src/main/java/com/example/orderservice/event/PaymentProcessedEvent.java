package com.example.orderservice.event;

import lombok.*;

import java.math.BigDecimal;
import java.time.LocalDateTime;

/**
 * Response event published after payment validation
 *
 * Consumed by order-service to determine final order status
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class PaymentProcessedEvent {

    private String orderId;
    private String customerId;
    private BigDecimal amount;

    /**
     * Payment status: ACCEPT or REJECT
     */
    private PaymentStatus status;

    /**
     * Reason for rejection (if status=REJECT)
     */
    private String reason;

    private LocalDateTime processedAt;

    public enum PaymentStatus {
        ACCEPT,   // Sufficient balance, funds reserved
        REJECT    // Insufficient balance
    }
}
