package com.example.stockconsumer.kafka.dto;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class StockConsumer {


    private final KafkaTemplate<String, String> kafkaTemplate;

    private final String STOCK_TOPIC = "stock-result";

    @KafkaListener(
            topics = "reservation_requested",
            groupId = "stock-group"
    )
    public void consume(){

    }



}
