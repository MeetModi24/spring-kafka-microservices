package com.example.stockservice.model;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import java.math.BigDecimal;

@Entity
@Table(name = "products")
@Data
@NoArgsConstructor
@AllArgsConstructor
public class Product {
    
    @Id
    private String productId;
    
    private String productName;
    
    private Integer availableItems;  // Available for new orders
    
    private Integer reservedItems;   // Reserved for pending orders
    
    private BigDecimal price;
    
    /**
     * Reserve stock for an order.
     * 
     * @param quantity Quantity to reserve
     * @return true if reservation successful, false if insufficient stock
     */
    public boolean reserve(int quantity) {
        if (availableItems >= quantity) {
            availableItems -= quantity;
            reservedItems += quantity;
            return true;
        }
        return false;
    }
    
    /**
     * Confirm reservation (commit the reservation).
     * Moves items from reserved to permanently deducted.
     */
    public void confirm(int quantity) {
        if (reservedItems >= quantity) {
            reservedItems -= quantity;
            // Items are now sold (not returned to available)
        }
    }
    
    /**
     * Rollback reservation (compensating transaction).
     * Returns reserved items back to available pool.
     */
    public void rollback(int quantity) {
        if (reservedItems >= quantity) {
            reservedItems -= quantity;
            availableItems += quantity;
        }
    }
}