package com.example.producer.kafka;

import com.example.producer.domain.reservation.ReservationService;
import com.example.producer.kafka.dto.KafkaEventStockResult;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class ReservationConsumer {

    private final String STOCK_TOPIC = "stock-result";

    private final ReservationService reservationService;

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

        // 예매 내역 상태 업데이트 성공
        else {
            log.info("예매 내역이 성공적으로 업데이트 되었습니다. 변경된 예약 내역 => {}", event.getReservationStatus());
        }

    }

}
