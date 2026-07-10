package com.example.stockservice.event;

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
     * Source of failure - "PAYMENT" or "STOCK" (Phase 5 only)
     * Used for ROLLBACK decisions to identify which service failed
     */
    private String source;

    /**
     * Timestamp when decision was made
     */
    private LocalDateTime decidedAt;

    /**
     * Decision status enum
     *
     * CONFIRMED: All participants accepted - commit transaction
     * REJECTED: All participants rejected - nothing to compensate
     * ROLLBACK: Partial success - compensate the successful participant (Phase 5)
     */
    public enum DecisionStatus {
        /**
         * Order confirmed by orchestrator - commit the reservation
         */
        CONFIRMED,

        /**
         * Order rejected by orchestrator - both services failed, nothing to compensate
         */
        REJECTED,

        /**
         * Partial success - one service succeeded, one failed
         * The successful service must rollback (compensate)
         * The 'source' field identifies which service failed
         */
        ROLLBACK
    }
}
