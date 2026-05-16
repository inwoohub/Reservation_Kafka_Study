package com.example.producer.domain.reservation.dto;


import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Getter
@AllArgsConstructor
@NoArgsConstructor
public class GetReservationRequest {

    private String buyerName; // 구매자 이름

    private String birthDate; // 생,년,월,일

    private String teamPassword; // 비회원 주문 조회용 임시 비밀번호

}
