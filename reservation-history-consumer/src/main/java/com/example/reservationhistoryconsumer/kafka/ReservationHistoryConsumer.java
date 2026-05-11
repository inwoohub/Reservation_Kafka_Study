package com.example.reservationhistoryconsumer.kafka;

import com.example.reservationhistoryconsumer.kafka.dto.KafkaEventReservation;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Producer 는 KafkaTemplate 을 통해서 send 로 로그를 올렸다면,
 * Consumer 는 @KafkaListener 을 통해서 구독을 하고 메시지를 읽는다.
 */
@Slf4j
@Component
public class ReservationHistoryConsumer {

    @KafkaListener(
            topics = "create-reservation",
            groupId = "reservation-history-group"
    )

    public void consume(KafkaEventReservation event){
        log.info("=======카프카 이벤트 수신 확인=======");
        log.info("eventId = {}", event.getEventId());
        log.info("orderId = {}", event.getOrderId());
        log.info("productId = {}", event.getProductId());
        log.info("quantity = {}", event.getQuantity());
        log.info("totalPrice = {}", event.getTotalPrice());
        log.info("buyerName = {}", event.getBuyerName());
        log.info("staus = {}", event.getStatus());
        log.info("timestamp = {}", event.getTimestamp());
        log.info("================================");
    }


}
