package com.example.reservationhistoryconsumer.domain.reservation;


import com.example.reservationhistoryconsumer.kafka.dto.KafkaEventStockResult;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Slf4j
@Service
@RequiredArgsConstructor
@Transactional(readOnly = true)
public class ReservationService {

    private final ReservationRepository reservationRepository;

    // 스탁 결과로 예약 상태 변경하기
    @Transactional
    public void stockResult(KafkaEventStockResult event) {

        log.info("🔥Stock result for reservation {}", event.getBuyerName());

        // 1. ID 로 예약 내역 조회
        Reservation reservation = reservationRepository.findById(event.getId())
                .orElseThrow(() -> new IllegalArgumentException("존재하지 않는 예약 ID입니다."));

        // 2. 예약 내역 상태 변경 (더티 체킹으로 save, update 안해줘도 됨!)
        reservation.setReservationStatus(event.getReservationStatus());

        log.info("예약 상태 변경 완료! -> {} 🤒", reservation.getReservationStatus());

    }

    @Transactional
    public void stockResultV2(KafkaEventStockResult event) {

        // 여기 쿼리도 영속성 컨텍스트를 사용하지 않고, 원자 처리
        boolean check = reservationRepository.setReservationStatus(event.getId(), event.getReservationStatus());

        // 1. id 가 잘못되었거나, 다른 오류가 있는경우
        if(!check){
            log.info("stockResultV2 - 잘못된 요청입니다.");
            return;
        }

        else{
            log.info("예약 상태 변경 완료! -> {} 🤒", event.getReservationStatus());
        }

    }

    @Transactional
    public void stockResultV3(KafkaEventStockResult event) {

        // 1. 여기서 처음으로 데이터 저장
        Reservation reservation = new Reservation(event);
        reservationRepository.save(reservation);

        log.info("예약 상태 변경 완료! -> {} 🤒", event.getReservationStatus());

    }

    // 저장하기
    @Transactional
    public Reservation saveReservation(Reservation reservation) {
        return reservationRepository.save(reservation);
    }

}

