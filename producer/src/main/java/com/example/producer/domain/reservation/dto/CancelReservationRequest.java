package com.example.producer.domain.reservation.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class CancelReservationRequest {

    private Long id;

    private String teamPassword; // 비회원 주문 조회용 임시 비밀번호


}
