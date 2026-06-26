package com.example.orderservice.dto;

import java.math.BigDecimal;

/**
 * ORDER ITEM RESPONSE DTO
 * ========================
 *
 * Represents an order item in the response.
 * Same structure as request for this simple case,
 * but in real apps might include:
 * - stockStatus
 * - availabilityDate
 * - discountApplied
 */
public class OrderItemResponse {

    private String productId;
    private String productName;
    private int quantity;
    private BigDecimal price;

    public OrderItemResponse() {
    }

    public OrderItemResponse(String productId, String productName, int quantity, BigDecimal price) {
        this.productId = productId;
        this.productName = productName;
        this.quantity = quantity;
        this.price = price;
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
        return "OrderItemResponse{" +
                "productId='" + productId + '\'' +
                ", productName='" + productName + '\'' +
                ", quantity=" + quantity +
                ", price=" + price +
                '}';
    }
}
