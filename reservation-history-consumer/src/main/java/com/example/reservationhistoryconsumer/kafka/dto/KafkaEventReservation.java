package com.example.reservationhistoryconsumer.kafka.dto;

import com.example.reservationhistoryconsumer.domain.reservation.ReservationStatus;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Producer 에서 해당 객체를 JSON 으로 카프카 Value 에 넣어둠
 * 따라서 해당 DTO 그대로 가져와서 사용할 수 있도록 똑같이 만들어줌.
 */

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

    // 상품 판매중 or 판매 완료
    private ProductStatus productStatus;

    // 비회원 구매자 정보
    private String buyerName;
    private String birthDate;
    private String tempPassword;

    // 상태 및 시간
    private ReservationStatus reservationStatus;       // 상태 (예: "PURCHASE_REQUESTED")
    private Long timestamp;                            // 이벤트 발생 시간

}