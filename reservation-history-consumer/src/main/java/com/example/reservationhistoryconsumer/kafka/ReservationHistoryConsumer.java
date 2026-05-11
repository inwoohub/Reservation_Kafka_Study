package com.example.reservationhistoryconsumer.kafka;

import com.example.reservationhistoryconsumer.domain.reservation.Reservation;
import com.example.reservationhistoryconsumer.domain.reservation.ReservationRepository;
import com.example.reservationhistoryconsumer.kafka.dto.KafkaEventReservation;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Producer 는 KafkaTemplate 을 통해서 send 로 로그를 올렸다면,
 * Consumer 는 @KafkaListener 을 통해서 구독을 하고 메시지를 읽는다.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class ReservationHistoryConsumer {

    private final ReservationRepository reservationRepository;

    @KafkaListener(
            topics = "create-reservation",
            groupId = "reservation-history-group"
    )

    public void consume(KafkaEventReservation event) {

        // 1. 카프카에서 불러온 데이터 읽기 -> event
        Reservation reservation = new Reservation(event);

        // 2. DB에 저장하기
        reservationRepository.save(reservation);

    }


}
