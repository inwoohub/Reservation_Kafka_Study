package com.example.producer.kafka.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * 주문 취소용 DTO
 */

@Data
@AllArgsConstructor
@NoArgsConstructor
public class KafkaEventReservationCancel {

    private Long reservationId; // 주문 ID

    private Long productId; // 상품 고유 ID

    private Integer quantity; // 취소 수량

}
