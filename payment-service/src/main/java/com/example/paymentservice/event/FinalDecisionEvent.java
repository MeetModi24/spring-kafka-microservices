package com.example.paymentservice.event;

import lombok.*;

import java.time.LocalDateTime;

/**
 * Final decision event received from order-service via Kafka
 *
 * IMPORTANT: Must match order-service's FinalDecisionEvent structure exactly
 * for JSON deserialization to work
 *
 * PURPOSE:
 * - Sent by order-service after aggregating all participant responses
 * - Tells each participant to CONFIRM (commit) or ROLLBACK (compensate)
 * - Enables SAGA pattern completion phase
 *
 * KAFKA PATTERN:
 * - Same topic as OrderCreatedEvent ("order-events")
 * - Different consumer group ("payment-decision-group")
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
     * CONFIRMED: All participants accepted → commit transaction
     * REJECTED: At least one participant rejected → compensate/rollback
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
