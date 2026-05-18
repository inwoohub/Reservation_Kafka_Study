package com.example.producer.domain.reservation;

import com.example.producer.domain.reservation.dto.CancelReservationRequest;
import com.example.producer.domain.reservation.dto.CreateReservationRequest;
import com.example.producer.domain.reservation.dto.GetReservationRequest;
import com.example.producer.domain.reservation.dto.ReservationResponse;
import com.example.producer.global.error.ApiException;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@Tag(name = "2. ReservationController", description = "콘서트 예약, 조회, 취소 API")
@RestController
@RequestMapping("/api/reservation")
@RequiredArgsConstructor
public class ReservationController {

    private final ReservationService reservationService;

    @Operation(summary = "예매하기")
    @PostMapping
    public ResponseEntity<String> createReservation(@RequestBody CreateReservationRequest req) {
//        reservationService.addReservation(req); // 이전 버전
//        reservationService.addReservationV2(req); // 성능 개선 버전
        boolean result = reservationService.addReservationV3(req);// 최종 아키텍처 버전
        if (!result) {
            throw new ApiException(HttpStatus.BAD_REQUEST, "400", "BAD_REQUEST", "예약이 실패하였습니다.");
        }
        return ResponseEntity.status(202).body("예약 신청이 완료되었습니다.");
    }

//    @Operation(summary = "예매 조회하기")
//    @PostMapping("/get")
    public ResponseEntity<List<Reservation>> getAllReservations(@RequestBody GetReservationRequest req) {
        List<Reservation> allReservations = reservationService.getAllReservations(req);
        return ResponseEntity.status(HttpStatus.OK).body(allReservations);
    }

    /**
     * 기존 예매하기 조회는 Reservation 만 반환하기 때문에
     * 제품명같은게 담기지 않았음.
     * 따라서 제품 정보또 한 담아주기 위해서 DTO 만들어서 반환하자.
     */
    @Operation(summary = "예매 조회하기")
    @PostMapping("/get")
    public ResponseEntity<List<ReservationResponse>> getAllReservationsV2(@RequestBody GetReservationRequest req) {
        List<ReservationResponse> response = reservationService.getAllReservationsV2(req);
        return ResponseEntity.status(HttpStatus.OK).body(response);
    }

    @Operation(summary = "예매 취소하기")
    @PostMapping("/cancel")
    public ResponseEntity<Void> cancelReservation(@RequestBody CancelReservationRequest req) {
        reservationService.cancelReservation(req);
        return ResponseEntity.status(HttpStatus.OK).build();
    }


}
