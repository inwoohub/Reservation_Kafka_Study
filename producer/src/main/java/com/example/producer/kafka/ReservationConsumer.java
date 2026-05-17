package com.example.producer.kafka;

import com.example.producer.domain.reservation.ReservationService;
import com.example.producer.domain.reservation.dto.ReservationStatus;
import com.example.producer.kafka.dto.KafkaEventStockResult;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.script.RedisScript;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.List;

@Slf4j
@Component
@RequiredArgsConstructor
public class ReservationConsumer {

    private final String STOCK_TOPIC = "stock-result";

    private final ReservationService reservationService;

    private final RedisTemplate<String, Object> redisTemplate;

    private final RedisScript<Long> increaseStockScript; // RedisConfig 에서 Bean으로 만든 스크립트 주입 받기

    private static final String PRODUCT_STOCK_PREFIX = "product:stock:";


    @KafkaListener(
            topics = STOCK_TOPIC,
            groupId = "reservation-group"
    )
    public void reservationConsume(KafkaEventStockResult event) {

        // 1. 예매 내역 상태 업데이트
        boolean result = reservationService.updateStatus(event);


        // 예매 내역 상태 업데이트 실패
        if (!result) {
            log.info("업데이트 과정에서 오류가 발생했습니다.");
        }

        // 2. 상태가 만약 취소 요청 성공이라면
        if(event.getReservationStatus().equals(ReservationStatus.CANCEL_COMPLETED)){
            // 레디스에서 수량도 증가 시키기
            Long stockServiceCheck = redisTemplate.execute(
                    increaseStockScript,
                    List.of(PRODUCT_STOCK_PREFIX + event.getProductId()),
                    event.getQuantity()
            );

            if(stockServiceCheck == -1L){
                log.info("해당 하는 재고가 없어 재고 감소가 실패했습니다.");
            } else if(stockServiceCheck == 0L){
                log.info("해당 하는 재고가 없어 재고 감소가 실패했습니다.");
            } else if(stockServiceCheck == 1L){
                log.info("예매 취소 신청이 성공적으로 업데이트 되었습니다. 변경된 예약 내역 => {}", event.getReservationStatus());
            }

        }

        // 예매 내역 상태 업데이트 성공
        else {
            log.info("예매 내역이 성공적으로 업데이트 되었습니다. 변경된 예약 내역 => {}", event.getReservationStatus());
        }

    }

}
