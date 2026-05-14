package com.example.reservationhistoryconsumer.domain.reservation.querydsl;

import com.example.reservationhistoryconsumer.domain.reservation.ReservationStatus;

public interface ReservationRepositoryQuerydsl {

    boolean setReservationStatus(Long id, ReservationStatus reservationStatus);

}
