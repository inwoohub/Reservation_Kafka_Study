package com.example.reservationhistoryconsumer.kafka;

import com.example.reservationhistoryconsumer.domain.reservation.Reservation;
import com.example.reservationhistoryconsumer.domain.reservation.ReservationService;
import com.example.reservationhistoryconsumer.kafka.dto.KafkaEventReservation;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

/**
 * Producer 는 KafkaTemplate 을 통해서 send 로 로그를 올렸다면,
 * Consumer 는 @KafkaListener 을 통해서 구독을 하고 메시지를 읽는다.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class ReservationHistoryConsumer {

    private final KafkaTemplate<String, Reservation> reservationKafkaTemplate;

    private final ReservationService reservationService;

    private final String CREATE_TOPIC = "reservation_created"; // 토픽을 reservation_created

    @KafkaListener(
            topics = "reservation_requested",
            groupId = "reservation-history-group"
    )
    public void createReservation(KafkaEventReservation event) {

        // 1. 예매 내역 저장 할 수 있는 객체로 변환
        Reservation reservation = new Reservation(event);

        // 2. 저장하기
        Reservation save = reservationService.saveReservation(reservation);

        // 3. Consumer가 Producer의 역할도 하기 (여기서 메시지 발행)
        reservationKafkaTemplate.send(CREATE_TOPIC, save);

    }

    @KafkaListener(
            topics = "stock-result",
            groupId = "stock-result-group"
    )
    public void stockResult(Reservation event) {
        // 1. 스탁 결과에서 예약 상태 가져와서 예약 상태 변경 시키기
        reservationService.stockResult(event);
    }


}
