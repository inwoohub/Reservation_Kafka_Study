package com.example.producer.domain.reservation;

import com.example.producer.domain.reservation.dto.CreateReservationRequest;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@Tag(name = "2. ReservationController", description = "콘서트 예약, 조회, 취소 API")
@RestController
@RequestMapping("/api/reservation")
@RequiredArgsConstructor
public class ReservationController {

    private final ReservationService reservationService;

    @Operation(summary = "예매하기")
    @PostMapping
    public ResponseEntity<Void> createReservation(@RequestBody CreateReservationRequest req) {
//        reservationService.addReservation(req); // 이전 버전
        reservationService.addReservationV2(req); // 성능 개선 버전
        return ResponseEntity.status(HttpStatus.CREATED).build();
    }


}
