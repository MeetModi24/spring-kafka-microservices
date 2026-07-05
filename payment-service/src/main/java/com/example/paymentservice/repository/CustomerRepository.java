package com.example.paymentservice.repository;

import com.example.paymentservice.model.Customer;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.Optional;

/**
 * JPA Repository for Customer entity
 * 
 * DESIGN PATTERN: Repository Pattern
 * - Abstracts data access logic
 * - Provides CRUD operations out-of-the-box
 * - Supports custom query methods
 */
@Repository
public interface CustomerRepository extends JpaRepository<Customer, Long> {
    
    /**
     * Find customer by business ID (e.g., "CUST-123")
     * 
     * Spring Data JPA auto-generates implementation from method name:
     * - "findBy" → SELECT query
     * - "CustomerId" → WHERE customer_id = ?
     * 
     * @param customerId Business customer ID
     * @return Optional<Customer> (empty if not found)
     */
    Optional<Customer> findByCustomerId(String customerId);
}