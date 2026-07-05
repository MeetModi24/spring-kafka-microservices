package com.example.paymentservice;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Payment Service - Kafka Consumer Microservice
 * 
 * NO @EnableWebMvc or REST controllers
 * This is a pure Kafka consumer service with JPA persistence
 * 
 * On startup:
 * 1. Connects to H2 database (jdbc:h2:mem:paymentdb)
 * 2. Creates schema (customers table)
 * 3. Initializes 10 test customers (@PostConstruct in DataInitializer)
 * 4. Connects to Kafka broker (localhost:9092)
 * 5. Subscribes to "order-events" topic
 * 6. Waits for messages...
 */
@SpringBootApplication
public class PaymentServiceApplication {
    
    public static void main(String[] args) {
        SpringApplication.run(PaymentServiceApplication.class, args);
    }
}