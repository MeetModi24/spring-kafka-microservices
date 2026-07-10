package com.example.orderservice.model;

/**
 * ORDER STATUS - Enum
 * ====================
 *
 * DESIGN PATTERN: State Pattern (simplified)
 * - Represents the lifecycle states of an Order
 * - Type-safe (compiler prevents invalid states)
 * - Better than String status (no typos like "PENDNG")
 *
 * STATE TRANSITIONS:
 * PENDING → CONFIRMED → PROCESSING → SHIPPED → DELIVERED
 *        ↓
 *    CANCELLED
 *
 * In a full State Pattern implementation, each state would be a class
 * with transition logic. For simple cases, enum is sufficient.
 */
public enum OrderStatus {
    /**
     * Order created but not yet confirmed
     */
    PENDING,

    /**
     * Order confirmed by customer
     */
    CONFIRMED,

    /**
     * Payment successful, order being processed
     */
    PROCESSING,

    /**
     * Order shipped to customer
     */
    SHIPPED,

    /**
     * Order delivered to customer
     */
    DELIVERED,

    /**
     * Order cancelled (by customer or system)
     */
    CANCELLED,

    /**
     * Order rejected (SAGA decision - payment or stock failed)
     */
    REJECTED,

    /**
     * Order requires rollback (SAGA decision - partial success, compensation needed)
     */
    ROLLBACK
}
