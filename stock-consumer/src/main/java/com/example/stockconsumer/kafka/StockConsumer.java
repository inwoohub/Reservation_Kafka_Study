package com.example.stockconsumer.kafka;

import com.example.stockconsumer.domain.product.ProductService;
import com.example.stockconsumer.kafka.dto.KafkaEventReservation;
import com.example.stockconsumer.kafka.dto.Reservation;
import com.example.stockconsumer.kafka.dto.ReservationStatus;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.util.List;

@Slf4j
@Component
@RequiredArgsConstructor
public class StockConsumer {


    private final KafkaTemplate<String, Reservation> kafkaTemplate;

    private final String STOCK_TOPIC = "stock-result";

    private final RedisScript<Long> decreaseStockScript; // RedisConfig 에서 Bean으로 만든 스크립트 주입 받기

    private final ProductService productService;

    private final RedisTemplate<String, Object> redisTemplate;

    private static final String PRODUCT_STOCK_PREFIX = "product:stock:";
    private final KafkaAdmin kafkaAdmin;


    /**
     * 예약 내역 이벤트 올라왔을 때
     *
     * 1. 재고 확인 및 차감
     * 2. stock-result 이벤트 발행하기
     */
//    @KafkaListener(
//            topics = "reservation_created",
//            groupId = "stock-group"
//    )
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
     * V2. 성능 개선을 위해서 재고 차감을 원자적 처리
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


    /**
     * V3. 레디스 Lua Script 를 통해 DB로 접근하지 않고, Redis 에서만 재고 차감 후
     *     성공 or 실패 이벤트 발행
     *
     * 기존 : DB 연산 작업이 1회 있었음
     * 변경 : DB 연산 작업 x, 대신 Memory DB 인 Redis 에서 연산 작업 1회로 성능 올리기
     *
     * -1 : 재고 없음
     *  0 : 재고보다 주문 수량이 많음
     *  1 : 주문 성공
     */
    @KafkaListener(
            topics = "reservation_requested",
            groupId = "stock-group"
    )
    public void reservationSuccessV3(KafkaEventReservation event){

        // 1. Redis 에서 Lua Script 활용해서 재고 차감
        Long stockServiceCheck = redisTemplate.execute(
                decreaseStockScript,
                List.of(PRODUCT_STOCK_PREFIX + event.getProductId()),
                String.valueOf(event.getQuantity())
        );

        // 2. 결과 확인하기
        Reservation reservation = new Reservation();
        if(stockServiceCheck == -1L){ // 예약 실패 : 존재하지 않는 상품임
            reservation = new Reservation(event, ReservationStatus.PURCHASE_FAILED);
        }

        else if (stockServiceCheck == 0L){ // 예약 실패 : 품절된 상태
            reservation = new Reservation(event, ReservationStatus.PURCHASE_FAILED);
        }

        else if (stockServiceCheck == 1L){ // 주문 성공 :
            reservation = new Reservation(event, ReservationStatus.PURCHASE_CONFIRMED);
        }

        // 3. 이벤트로 주문 내역 발행
        kafkaTemplate.send(STOCK_TOPIC, reservation);

    }


}
