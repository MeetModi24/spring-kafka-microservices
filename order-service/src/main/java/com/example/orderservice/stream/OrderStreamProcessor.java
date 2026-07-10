package com.example.orderservice.stream;

import com.example.orderservice.event.FinalDecisionEvent;
import com.example.orderservice.event.PaymentProcessedEvent;
import com.example.orderservice.event.StockProcessedEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.support.serializer.JsonSerde;

import java.time.Duration;
import java.time.LocalDateTime;

/**
 * Kafka Streams processor for SAGA orchestration.
 *
 * PURPOSE:
 * - Joins payment-events and stock-events streams
 * - Makes final decision based on both responses
 * - Publishes FinalDecisionEvent to order-events topic
 *
 * KAFKA STREAMS CONCEPTS:
 * - KStream: Unbounded stream of records (events)
 * - Join: Combines two streams based on key and time window
 * - JoinWindows: Time-based window for matching events
 * - Serdes: Serializer/Deserializer for custom types
 *
 * DECISION LOGIC:
 * - CONFIRMED: Both Payment=ACCEPT AND Stock=ACCEPT
 * - REJECTED: Both Payment=REJECT AND Stock=REJECT
 * - ROLLBACK: One ACCEPT, one REJECT (partial success needs compensation)
 */
@Configuration
@EnableKafkaStreams
@RequiredArgsConstructor
@Slf4j
public class OrderStreamProcessor {

    /**
     * Creates Kafka Streams topology for SAGA orchestration.
     *
     * TOPOLOGY:
     * 1. Create payment-events KStream (keyed by orderId)
     * 2. Create stock-events KStream (keyed by orderId)
     * 3. Join both streams with 10-second window
     * 4. Apply decision logic
     * 5. Publish to order-events topic
     *
     * @param streamsBuilder StreamsBuilder injected by Spring
     * @return KStream topology (Spring will auto-start it)
     */
    @Bean
    public KStream<String, FinalDecisionEvent> orderDecisionStream(StreamsBuilder streamsBuilder) {

        // Create ObjectMapper for JSON serialization (with LocalDateTime support)
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.registerModule(new JavaTimeModule());

        // Create Serdes for custom event types
        JsonSerde<PaymentProcessedEvent> paymentSerde = new JsonSerde<>(PaymentProcessedEvent.class, objectMapper);
        JsonSerde<StockProcessedEvent> stockSerde = new JsonSerde<>(StockProcessedEvent.class, objectMapper);
        JsonSerde<FinalDecisionEvent> decisionSerde = new JsonSerde<>(FinalDecisionEvent.class, objectMapper);

        // STEP 1: Create KStream for payment-events
        // Key: orderId, Value: PaymentProcessedEvent
        KStream<String, PaymentProcessedEvent> paymentStream = streamsBuilder
                .stream(
                    "payment-events",
                    Consumed.with(Serdes.String(), paymentSerde)
                )
                .peek((key, value) -> log.info("Received payment event: orderId={}, status={}",
                    value.getOrderId(), value.getStatus()));

        // STEP 2: Create KStream for stock-events
        // Key: orderId, Value: StockProcessedEvent
        KStream<String, StockProcessedEvent> stockStream = streamsBuilder
                .stream(
                    "stock-events",
                    Consumed.with(Serdes.String(), stockSerde)
                )
                .peek((key, value) -> log.info("Received stock event: orderId={}, status={}",
                    value.getOrderId(), value.getStatus()));

        // STEP 3: Join both streams with 10-second window
        // JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofSeconds(10)):
        // - Waits up to 10 seconds for matching event from other stream
        // - No grace period (strict window)
        // - If one event arrives but other doesn't arrive within 10s, join doesn't happen
        KStream<String, FinalDecisionEvent> decisionStream = paymentStream.join(
                stockStream,
                this::makeDecision,  // ValueJoiner function
                JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofSeconds(10)),
                StreamJoined.with(
                    Serdes.String(),      // Key serde (orderId)
                    paymentSerde,         // Left value serde (PaymentProcessedEvent)
                    stockSerde            // Right value serde (StockProcessedEvent)
                )
        );

        // STEP 4: Log decision and publish to order-events
        decisionStream
                .peek((key, value) -> log.info(
                    "Final decision made: orderId={}, status={}, reason={}",
                    value.getOrderId(),
                    value.getStatus(),
                    value.getReason()
                ))
                .to("order-events", Produced.with(Serdes.String(), decisionSerde));

        return decisionStream;
    }

    /**
     * Decision logic for SAGA orchestration.
     *
     * RULES:
     * - CONFIRMED: Payment=ACCEPT AND Stock=ACCEPT
     * - REJECTED: Payment=REJECT AND Stock=REJECT
     * - ROLLBACK: One ACCEPT, one REJECT
     *
     * ROLLBACK SCENARIOS:
     * - Payment=ACCEPT, Stock=REJECT → Rollback payment (source="STOCK")
     * - Payment=REJECT, Stock=ACCEPT → Rollback stock (source="PAYMENT")
     *
     * @param paymentEvent Payment service response
     * @param stockEvent Stock service response
     * @return FinalDecisionEvent with orchestrator decision
     */
    private FinalDecisionEvent makeDecision(
            PaymentProcessedEvent paymentEvent,
            StockProcessedEvent stockEvent) {

        boolean paymentAccepted = paymentEvent.getStatus() == PaymentProcessedEvent.PaymentStatus.ACCEPT;
        boolean stockAccepted = stockEvent.getStatus() == StockProcessedEvent.StockStatus.ACCEPT;

        FinalDecisionEvent.FinalDecisionEventBuilder builder = FinalDecisionEvent.builder()
                .orderId(paymentEvent.getOrderId())
                .customerId(paymentEvent.getCustomerId())
                .amount(paymentEvent.getAmount())
                .decidedAt(LocalDateTime.now());

        // CASE 1: Both accepted → CONFIRMED
        if (paymentAccepted && stockAccepted) {
            log.info("Order {} CONFIRMED: Both payment and stock accepted", paymentEvent.getOrderId());
            return builder
                    .status(FinalDecisionEvent.DecisionStatus.CONFIRMED)
                    .reason("Order confirmed: All services accepted")
                    .build();
        }

        // CASE 2: Both rejected → REJECTED (nothing to compensate)
        if (!paymentAccepted && !stockAccepted) {
            log.warn("Order {} REJECTED: Both payment and stock rejected", paymentEvent.getOrderId());
            return builder
                    .status(FinalDecisionEvent.DecisionStatus.REJECTED)
                    .reason(String.format("Payment: %s; Stock: %s",
                        paymentEvent.getReason(),
                        stockEvent.getReason()))
                    .build();
        }

        // CASE 3: Payment accepted, Stock rejected → ROLLBACK payment
        if (paymentAccepted && !stockAccepted) {
            log.warn("Order {} ROLLBACK: Payment accepted but stock unavailable. Source: STOCK",
                paymentEvent.getOrderId());
            return builder
                    .status(FinalDecisionEvent.DecisionStatus.ROLLBACK)
                    .reason("Stock unavailable: " + stockEvent.getReason())
                    .source("STOCK")  // Stock service failed, payment needs rollback
                    .build();
        }

        // CASE 4: Payment rejected, Stock accepted → ROLLBACK stock
        log.warn("Order {} ROLLBACK: Stock reserved but payment failed. Source: PAYMENT",
            paymentEvent.getOrderId());
        return builder
                .status(FinalDecisionEvent.DecisionStatus.ROLLBACK)
                .reason("Payment failed: " + paymentEvent.getReason())
                .source("PAYMENT")  // Payment service failed, stock needs rollback
                .build();
    }
}
