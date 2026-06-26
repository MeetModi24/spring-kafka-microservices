package com.example.orderservice.model;

import java.math.BigDecimal;

/**
 * ORDER ITEM - Value Object
 * ==========================
 *
 * DESIGN PATTERN: Value Object (DDD)
 * - No unique identity (identified by parent Order)
 * - Immutable (should not change once created)
 * - Equality based on values, not identity
 *
 * WHY?
 * - An OrderItem doesn't exist independently - it's part of an Order
 * - Two OrderItems with same productId and quantity are considered equal
 * - No need for separate OrderItem table/ID in many cases
 */
public class OrderItem {

    private String productId;
    private String productName;
    private int quantity;
    private BigDecimal price; // Price per unit

    // Default constructor (for Jackson deserialization)
    public OrderItem() {
    }

    public OrderItem(String productId, String productName, int quantity, BigDecimal price) {
        this.productId = productId;
        this.productName = productName;
        this.quantity = quantity;
        this.price = price;
    }

    /**
     * Validation logic
     */
    public boolean isValid() {
        return productId != null && !productId.isBlank()
                && quantity > 0
                && price != null && price.compareTo(BigDecimal.ZERO) > 0;
    }

    // Getters and Setters
    public String getProductId() {
        return productId;
    }

    public void setProductId(String productId) {
        this.productId = productId;
    }

    public String getProductName() {
        return productName;
    }

    public void setProductName(String productName) {
        this.productName = productName;
    }

    public int getQuantity() {
        return quantity;
    }

    public void setQuantity(int quantity) {
        this.quantity = quantity;
    }

    public BigDecimal getPrice() {
        return price;
    }

    public void setPrice(BigDecimal price) {
        this.price = price;
    }

    @Override
    public String toString() {
        return "OrderItem{" +
                "productId='" + productId + '\'' +
                ", productName='" + productName + '\'' +
                ", quantity=" + quantity +
                ", price=" + price +
                '}';
    }
}
