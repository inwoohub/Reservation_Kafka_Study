package com.example.producer.kafka.dto;

import com.example.producer.domain.reservation.dto.ReservationStatus;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class KafkaEventStockResult {

    private Long reservationId;

    private Long productId;

    private ReservationStatus reservationStatus;

    private Integer quantity;

}
