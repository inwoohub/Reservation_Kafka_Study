package com.example.stockconsumer.kafka.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class KafkaEventStockResult {

    private Long reservationId;

    private ReservationStatus reservationStatus;

    private Integer quantity;

}
