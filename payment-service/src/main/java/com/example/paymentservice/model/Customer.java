package com.example.paymentservice.model;

import jakarta.persistence.*;
import lombok.*;

/**
 * Customer entity representing a customer account with balance management
 * 
 * BUSINESS LOGIC:
 * - amountAvailable: Current spendable balance
 * - amountReserved: Funds on hold (pending orders)
 * 
 * SAGA PATTERN:
 * - reserve(): Moves funds from available → reserved (tentative)
 * - confirm(): Deducts from reserved (commit)
 * - rollback(): Returns from reserved → available (compensate)
 */
@Entity
@Table(name = "customers")
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@ToString
public class Customer {
    
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;
    
    @Column(nullable = false)
    private String customerId;  // Business ID (e.g., "CUST-123")
    
    @Column(nullable = false)
    private String name;
    
    /**
     * Available balance (can be spent immediately)
     */
    @Column(nullable = false)
    private Integer amountAvailable = 0;
    
    /**
     * Reserved balance (pending order confirmation)
     */
    @Column(nullable = false)
    private Integer amountReserved = 0;
    
    /**
     * Reserve funds for an order (SAGA Reserve phase)
     * 
     * @param amount Amount to reserve
     * @return true if successful, false if insufficient balance
     */
    public boolean reserve(Integer amount) {
        if (amountAvailable >= amount) {
            amountAvailable -= amount;
            amountReserved += amount;
            return true;
        }
        return false;
    }
    
    /**
     * Confirm reservation - deduct from reserved (SAGA Confirm phase)
     * 
     * @param amount Amount to confirm
     */
    public void confirm(Integer amount) {
        if (amountReserved >= amount) {
            amountReserved -= amount;
        }
    }
    
    /**
     * Rollback reservation - return to available (SAGA Compensate phase)
     * 
     * @param amount Amount to rollback
     */
    public void rollback(Integer amount) {
        amountReserved -= amount;
        amountAvailable += amount;
    }
}