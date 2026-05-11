package com.example.producer.domain.reservation.dto;

import com.example.producer.domain.product.Product;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class KafkaEventReservation {

    private String eventId; // 카프카 이벤트 고유 ID (UUID)

    private String orderId; // 고객에게 보여줄 주문 번호 (UUID)

    // 상품 및 결제 정보
    private Long productId;      // 상품 ID
    private Integer quantity;    // 수량
    private Integer totalPrice;  // 총 결제 금액 (서버가 DB에서 단가를 조회해 계산해서 넣음!)

    // 비회원 구매자 정보
    private String buyerName;
    private String birthDate;
    private String tempPassword;

    // 상태 및 시간
    private ReservationStatus status;       // 상태 (예: "PURCHASE_REQUESTED")
    private Long timestamp;                 // 이벤트 발생 시간

    public KafkaEventReservation(String eventId, String orderId, CreateReservationRequest req, Product  product, ReservationStatus status, Long timestamp) {
        this.eventId = eventId;
        this.orderId = orderId;
        this.productId = req.getProductId();
        this.quantity = req.getQuantity();
        this.totalPrice = req.getQuantity() * product.getPrice();
        this.buyerName = req.getBuyerName();
        this.birthDate = req.getBirthDate();
        this.tempPassword = req.getTeamPassword();
        this.status = status;
        this.timestamp = timestamp;
    }
}
