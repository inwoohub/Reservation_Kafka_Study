package com.example.reservationhistoryconsumer.domain.reservation;

import com.example.reservationhistoryconsumer.kafka.dto.KafkaEventReservation;
import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.time.LocalDateTime;

@Table(name = "reservation")
@Entity @Getter
@NoArgsConstructor
@AllArgsConstructor
public class Reservation {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    private String eventId; // 이벤트 UUID

    private String orderId; // 주문 UUID

    private Long productId; // 상품 고유 ID

    private Integer quantity; // 주문 수량

    private Integer totalPrice; // 총 결제 금액

    private String buyerName; // 구매자 이름

    private String birthDate; // 구매가 생일 (6글자) ex) 001231

    private String tempPassword; // 주문 비밀번호 4자리 ex) 1234

    /**
     * @Enumerated 어노테이션을 통해서 enum 타입을 DB에 저장할 떄 타입을 지정해줄 수 있음
     * EnumType.STRING 을 통해 enum 타입을 String 타입으로 저장 가능
     */
    @Setter
    @Enumerated(EnumType.STRING)
    private ReservationStatus status; // ENUM 타입으로 주문 상태 관리

    private Long timestamp; // 이벤트 발생한 시간도 함께 저장하기

    private LocalDateTime createdAt; // 예약 내역이 생성된 시간

    public Reservation(KafkaEventReservation event) {
        this.eventId = event.getEventId();
        this.orderId = event.getOrderId();
        this.productId = event.getProductId();
        this.quantity = event.getQuantity();
        this.totalPrice = event.getTotalPrice();
        this.buyerName = event.getBuyerName();
        this.birthDate = event.getBirthDate();
        this.tempPassword = event.getTempPassword();
        this.status = event.getStatus();
        this.timestamp = event.getTimestamp();
        this.createdAt = LocalDateTime.now();
    }
}
