package com.example.paymentservice.config;

import com.example.paymentservice.model.Customer;
import com.example.paymentservice.repository.CustomerRepository;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Initializes database with test customers on application startup
 * 
 * @PostConstruct runs after all beans are initialized
 */
@Component
@RequiredArgsConstructor
@Slf4j
public class DataInitializer {
    
    private final CustomerRepository customerRepository;
    private final Random random = new Random();
    
    @PostConstruct
    public void init() {
        log.info("Initializing test customers...");
        
        List<Customer> customers = new ArrayList<>();
        
        // Create 10 test customers
        for (int i = 1; i <= 10; i++) {
            Customer customer = new Customer();
            customer.setCustomerId("CUST-" + i);
            customer.setName("Customer " + i);
            
            // Random balance between 1000 and 5000
            customer.setAmountAvailable(1000 + random.nextInt(4000));
            customer.setAmountReserved(0);
            
            customers.add(customer);
        }
        
        customerRepository.saveAll(customers);
        
        log.info("Created {} test customers", customers.size());
        customers.forEach(c -> log.info("  - {} | Balance: ${}", 
            c.getCustomerId(), c.getAmountAvailable()));
    }
}