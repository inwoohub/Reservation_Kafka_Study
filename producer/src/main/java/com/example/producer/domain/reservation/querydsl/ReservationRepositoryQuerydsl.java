package com.example.producer.domain.reservation.querydsl;


import com.example.producer.domain.reservation.dto.ReservationStatus;

public interface ReservationRepositoryQuerydsl {

    boolean setReservationStatus(Long id, ReservationStatus reservationStatus);

}
