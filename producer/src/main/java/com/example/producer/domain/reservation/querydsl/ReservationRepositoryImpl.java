package com.example.producer.domain.reservation.querydsl;


import com.example.producer.domain.reservation.Reservation;
import com.example.producer.domain.reservation.dto.ReservationStatus;
import com.querydsl.jpa.impl.JPAQueryFactory;
import lombok.RequiredArgsConstructor;

import java.util.List;

import static com.example.producer.domain.reservation.QReservation.reservation;

@RequiredArgsConstructor
public class ReservationRepositoryImpl implements ReservationRepositoryQuerydsl {

    private final JPAQueryFactory jpaQueryFactory;

    @Override
    public boolean setReservationStatus(Long id, ReservationStatus reservationStatus) {

        long updateCount = jpaQueryFactory
                .update(reservation)
                .set(reservation.reservationStatus, reservationStatus)
                .where(
                        reservation.id.eq(id)
                )
                .execute();

        return updateCount == 1L;

    }

    @Override
    public List<Reservation> getAllReservation(String buyerName, String birthDate, String teamPassword) {
        return jpaQueryFactory
                .select(reservation)
                .from(reservation)
                .where(
                        reservation.buyerName.eq(buyerName),
                        reservation.birthDate.eq(birthDate),
                        reservation.tempPassword.eq(teamPassword)
                )
                .fetch();
    }
}
