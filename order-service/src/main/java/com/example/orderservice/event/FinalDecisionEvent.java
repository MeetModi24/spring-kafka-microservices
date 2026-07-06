package com.example.orderservice.event;

import lombok.*;

import java.math.BigDecimal;
import java.time.LocalDateTime;

/**
 * Final decision event sent to all SAGA participants
 *
 * PURPOSE:
 * - Sent by order-service after aggregating all participant responses
 * - Tells each participant to CONFIRM (commit) or REJECT (compensate)
 * - Enables SAGA pattern completion phase
 *
 * KAFKA PATTERN:
 * - Published to "order-events" topic
 * - Different consumer group ("payment-decision-group") processes it
 * - Kafka delivers to BOTH groups independently
 * - Each message is processed by 2 different listeners
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class FinalDecisionEvent {

    /**
     * Order ID this decision applies to
     */
    private String orderId;

    /**
     * Customer ID for reference
     */
    private String customerId;

    /**
     * Order amount
     */
    private BigDecimal amount;

    /**
     * Final decision status: CONFIRMED or REJECTED
     */
    private DecisionStatus status;

    /**
     * Reason for rejection (if status=REJECTED)
     */
    private String reason;

    /**
     * Timestamp when decision was made
     */
    private LocalDateTime decidedAt;

    /**
     * Decision status enum
     *
     * CONFIRMED: All participants accepted - commit transaction
     * REJECTED: At least one participant rejected - compensate/rollback
     */
    public enum DecisionStatus {
        /**
         * Order confirmed by orchestrator - commit the reservation
         */
        CONFIRMED,

        /**
         * Order rejected by orchestrator - rollback the reservation
         */
        REJECTED
    }
}
