package com.example.stockconsumer.kafka.dto;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

@Getter
@NoArgsConstructor
@AllArgsConstructor
public class Reservation {

    private Long id;

    private String eventId; // 이벤트 UUID

    private String orderId; // 주문 UUID

    private Long productId; // 상품 고유 ID

    private Integer quantity; // 주문 수량

    private Integer totalPrice; // 총 결제 금액

    private String buyerName; // 구매자 이름

    private String birthDate; // 구매가 생일 (6글자) ex) 001231

    private String tempPassword; // 주문 비밀번호 4자리 ex) 1234

    private ReservationStatus reservationStatus; // ENUM 타입으로 주문 상태 관리

    private Long timestamp; // 이벤트 발생한 시간도 함께 저장하기

    private LocalDateTime createdAt; // 예약 내역이 생성된 시간

    // 구매 성공
    public void purchaseConfirmed() {
        this.reservationStatus = ReservationStatus.PURCHASE_CONFIRMED;
    }

}
