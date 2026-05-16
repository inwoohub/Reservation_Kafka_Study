package com.example.producer.domain.reservation.querydsl;


import com.example.producer.domain.reservation.Reservation;
import com.example.producer.domain.reservation.dto.ReservationStatus;

import java.util.List;

public interface ReservationRepositoryQuerydsl {

    boolean setReservationStatus(Long id, ReservationStatus reservationStatus);

    List<Reservation> getAllReservation(String buyerName, String birthDate, String teamPassword);

}
