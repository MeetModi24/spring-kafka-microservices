package com.example.stockservice.consumer;


@Component
@Slf4j
@RequiredArgsConstructor
public class OrderEventConsumer {

    private final StockService stockService;
    private final KafkaTemplate<String, Object> kafkaTemplate;

    private static final String STOCK_EVENTS_TOPIC = "stock-events";


    @KafkaListener(topics="order-events", groupId="stock-service-group")
    public void consumeOrderEvent(@Payload OrderCreatedEvent event,
        @Header(KafkaHeaders.RECEIVED_KEY) String key){
            log.info("Received OrderCreatedEvent: orderId={}, productId={}, quantity={}", 
                 event.getOrderId(), event.getItems().get(0).getProductId(), 
                 event.getItems().get(0).getQuantity());
            
    }

    
}
