package com.example.stockconsumer.kafka.dto;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

@Getter
@NoArgsConstructor
@AllArgsConstructor
public class Reservation {

//    private Long id; // 여기 단계까지 아직 저장되지않았기 때문에 제외

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

    public Reservation(KafkaEventReservation event, ReservationStatus reservationStatus) {
        this.eventId = event.getEventId();
        this.orderId = event.getOrderId();
        this.productId = event.getProductId();
        this.quantity = event.getQuantity();
        this.totalPrice = event.getTotalPrice();
        this.buyerName = event.getBuyerName();
        this.birthDate = event.getBirthDate();
        this.tempPassword = event.getTempPassword();
        this.reservationStatus = reservationStatus;
        this.timestamp = event.getTimestamp();
        this.createdAt = LocalDateTime.now();
    }


    public void purchaseFailed() {
        this.reservationStatus = ReservationStatus.PURCHASE_FAILED;
    }


    public void purchaseConfirmed() {
        this.reservationStatus = ReservationStatus.PURCHASE_CONFIRMED;
    }

}
