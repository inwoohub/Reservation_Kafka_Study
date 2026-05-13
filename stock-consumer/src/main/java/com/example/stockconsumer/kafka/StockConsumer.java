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
            topics = "reservation_created",
            groupId = "stock-group"
    )
    public void reservationSuccess(Reservation event){

        // 1. 재고 확인 및 차감
        boolean stockServiceCheck = productService.stockService(event);

        // 2. 만약 1번에서 실패했다면, 결과로 구매 실패로 상태 바꾸기
        if(!stockServiceCheck){
            event.purchaseFailed();
            log.info("{}님의 주문이 재고 확인 및 처리가 실패되었습니다!",event.getBuyerName());
        }

        // 3. 구매 성공했다면, 결과로 구매 성공으로 상태 바꾸기
        else {
            event.purchaseConfirmed(); // 구매 성공으로 상태 바꾸기
            log.info("{}님의 주문이 재고 확인 및 처리가 완료되었습니다!",event.getBuyerName());
        }

        // 4. stock-result 이벤트 발행하기
        kafkaTemplate.send(STOCK_TOPIC, event);

    }

    /**
     * 성능 개선을 위해서 재고 차감을 원자적 처리
     */
//    @KafkaListener(
//            topics = "reservation_created",
//            groupId = "stock-group"
//    )
    public void reservationSuccessV2(Reservation event){

        // 1. 재고 확인 및 차감
        boolean stockServiceCheck = productService.stockServiceV2(event);

        if(!stockServiceCheck){
            event.purchaseFailed();
            log.info("{}님의 주문이 재고 확인 및 처리가 실패되었습니다!",event.getBuyerName());
        }

        // 3. 구매 성공했다면, 결과로 구매 성공으로 상태 바꾸기
        else {
            event.purchaseConfirmed(); // 구매 성공으로 상태 바꾸기
            log.info("{}님의 주문이 재고 확인 및 처리가 완료되었습니다!",event.getBuyerName());
        }

        // 4. stock-result 이벤트 발행하기
        kafkaTemplate.send(STOCK_TOPIC, event);
    }


}
