package com.example.producer.domain.reservation.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class ReservationResponse {

    private Long reservationId; // 예약 고유 ID

    private String orderId;     // 주문 ID

    private Integer quantity;   // 주문 수량

    private Integer totalPrice; // 결제 총 가격

    private String buyerName;   // 구매자 이름

    private String birthDate;   // 구매자 생년월일

    private ReservationStatus reservationStatus; // 예약 상태

    private Long timestamp;     // 이벤트 발행한 시간

    private String productName; // 콘서트 명

    private LocalDateTime concertDateTime; // 콘서트 시간

    private LocalDateTime createdAt; // 해당 객체 만든 시간



}
