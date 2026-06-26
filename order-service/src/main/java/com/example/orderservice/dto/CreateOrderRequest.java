package com.example.orderservice.dto;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import java.util.List;

/**
 * CREATE ORDER REQUEST DTO
 * ========================
 *
 * DESIGN PATTERN: Data Transfer Object (DTO)
 * ==========================================
 *
 * WHAT IS A DTO?
 * - Object that carries data between processes (client ↔ server)
 * - Contains ONLY data, NO business logic
 * - Optimized for network transfer (only necessary fields)
 * - Independent from domain model (decouples API from internals)
 *
 * WHY USE DTOs?
 * =============
 * 1. API CONTRACT INDEPENDENCE
 *    - You can change Order domain model without breaking API
 *    - Example: Add internalNotes field to Order, but don't expose it
 *
 * 2. VALIDATION AT THE BOUNDARY
 *    - Validate input BEFORE it enters your domain
 *    - Use Bean Validation annotations (@NotNull, @Size, etc.)
 *    - Fail fast - reject bad requests early
 *
 * 3. SECURITY
 *    - Client can't set internal fields (orderId, createdAt, status)
 *    - DTO only has fields client is ALLOWED to send
 *    - Prevents mass assignment vulnerabilities
 *
 * 4. NETWORK OPTIMIZATION
 *    - Send only what's needed over the wire
 *    - Domain model might have 20 fields, DTO has 3
 *
 * 5. VERSIONING
 *    - Create OrderRequestV2 DTO for API v2
 *    - Map both V1 and V2 to same domain model
 *    - Supports multiple API versions simultaneously
 *
 * DTO vs DOMAIN MODEL:
 * ====================
 * DTO (CreateOrderRequest):
 * - customerId ✓
 * - items ✓
 *
 * Domain Model (Order):
 * - orderId ✗ (system-generated, not in DTO)
 * - customerId ✓
 * - items ✓
 * - totalAmount ✗ (calculated, not in DTO)
 * - status ✗ (internal state, not in DTO)
 * - createdAt ✗ (system-generated, not in DTO)
 * - internalNotes ✗ (internal field, not in DTO)
 *
 * WHEN TO USE DTO PATTERN?
 * =========================
 * ✓ REST APIs (always)
 * ✓ Kafka messages (event DTOs)
 * ✓ RPC calls (gRPC, SOAP)
 * ✗ Internal method calls within same service (use domain models)
 */
public class CreateOrderRequest {

    /**
     * Customer ID who is placing the order.
     *
     * VALIDATION: @NotBlank
     * - Not null
     * - Not empty string
     * - Not just whitespace
     *
     * WHY NOT @NotNull?
     * - @NotNull allows empty strings ("")
     * - @NotBlank is stricter for String fields
     */
    @NotBlank(message = "Customer ID is required")
    private String customerId;

    /**
     * List of items in the order.
     *
     * VALIDATION: @NotEmpty
     * - Not null
     * - Not empty list
     *
     * VALIDATION: @Valid
     * - Triggers validation on each OrderItemRequest in the list
     * - Cascading validation
     */
    @NotEmpty(message = "Order must contain at least one item")
    @Valid
    private List<OrderItemRequest> items;

    // ===================================
    // CONSTRUCTORS
    // ===================================

    /**
     * Default constructor (required by Jackson).
     *
     * WHY NEEDED?
     * Jackson deserializes JSON → Java object using:
     * 1. Default constructor (creates empty object)
     * 2. Setters (populates fields from JSON)
     *
     * Without default constructor, you'd get:
     * "Cannot construct instance of CreateOrderRequest"
     */
    public CreateOrderRequest() {
    }

    /**
     * All-args constructor (for testing and manual creation).
     */
    public CreateOrderRequest(String customerId, List<OrderItemRequest> items) {
        this.customerId = customerId;
        this.items = items;
    }

    // ===================================
    // GETTERS AND SETTERS
    // ===================================
    // Required for Jackson serialization/deserialization
    //
    // JACKSON SERIALIZATION (Java → JSON):
    // - Uses getters: getCustomerId() → "customerId" in JSON
    //
    // JACKSON DESERIALIZATION (JSON → Java):
    // - Uses setters: "customerId" in JSON → setCustomerId(value)
    //
    // NAMING CONVENTION:
    // - Field: customerId
    // - Getter: getCustomerId()
    // - Setter: setCustomerId(String customerId)
    //
    // Jackson maps using JavaBean convention:
    // - Remove "get"/"set" prefix
    // - Lowercase first letter
    // - Field name must match JSON key

    public String getCustomerId() {
        return customerId;
    }

    public void setCustomerId(String customerId) {
        this.customerId = customerId;
    }

    public List<OrderItemRequest> getItems() {
        return items;
    }

    public void setItems(List<OrderItemRequest> items) {
        this.items = items;
    }

    @Override
    public String toString() {
        return "CreateOrderRequest{" +
                "customerId='" + customerId + '\'' +
                ", items=" + items +
                '}';
    }
}
