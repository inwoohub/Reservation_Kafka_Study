package com.example.stockconsumer.kafka;

import com.example.stockconsumer.domain.product.ProductService;
import com.example.stockconsumer.kafka.dto.Reservation;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class StockConsumer {


    private final KafkaTemplate<String, Reservation> kafkaTemplate;

    private final String STOCK_TOPIC = "stock-result";

    private final ProductService productService;

    /**
     * 예약 내역 이벤트 올라왔을 때
     *
     * 1. 재고 확인 및 차감
     * 2. stock-result 이벤트 발행하기
     */
    @KafkaListener(
            topics = "reservation_requested",
            groupId = "stock-group"
    )
    public void consume(Reservation event){

        // 1. 재고 확인 및 차감
        productService.stockService(event);

        // 로그하나 찍자
        log.info("{}님의 주문이 재고 확인 및 처리가 완료되었습니다.!",event.getBuyerName());

        // 2. stock-result 이벤트 발행하기
        event.purchaseConfirmed(); // 구매 성공으로 상태 바꾸기
        kafkaTemplate.send(STOCK_TOPIC, event);

    }

}
