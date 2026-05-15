package com.example.stockconsumer.kafka.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class KafkaEventReservationRequest {

    private Long reservationId;

    private Long productId;

    private int quantity;

}
