package com.example.reservationhistoryconsumer.domain.reservation.querydsl;

import com.example.reservationhistoryconsumer.domain.reservation.ReservationStatus;
import com.querydsl.jpa.impl.JPAQueryFactory;
import lombok.RequiredArgsConstructor;

import static com.example.reservationhistoryconsumer.domain.reservation.QReservation.reservation;

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
