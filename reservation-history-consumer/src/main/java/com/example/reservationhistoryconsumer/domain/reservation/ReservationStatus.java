package com.example.reservationhistoryconsumer.domain.reservation;

public enum ReservationStatus {

    // 1. 구매(예약) 관련 상태
    PURCHASE_REQUESTED,     // 프로듀서(현재 서버)가 카프카에 이벤트를 쏠 때의 최초 상태
    PURCHASE_FAILED,        // 예약 실패 상태
    PURCHASE_CONFIRMED,     // 컨슈머(구독 서버)가 재고 차감에 성공했을 때의 상태
    OUT_OF_STOCK,           // 컨슈머가 확인해보니 재고가 없을 때의 상태

    // 2. 취소 관련 상태
    CANCEL_REQUESTED,       // 사용자가 취소 API를 호출했을 때
    CANCEL_COMPLETED        // 컨슈머가 재고를 다시 +1 하고 취소 처리 완료했을 때

}
