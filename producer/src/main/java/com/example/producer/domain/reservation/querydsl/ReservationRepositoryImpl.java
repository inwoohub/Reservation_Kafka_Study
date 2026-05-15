package com.example.producer.domain.reservation.querydsl;


import com.example.producer.domain.reservation.dto.ReservationStatus;
import com.querydsl.jpa.impl.JPAQueryFactory;
import lombok.RequiredArgsConstructor;

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
}
