package com.example.reservationhistoryconsumer.domain.reservation;

import com.example.reservationhistoryconsumer.domain.reservation.querydsl.ReservationRepositoryQuerydsl;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface ReservationRepository extends JpaRepository<Reservation, Long>, ReservationRepositoryQuerydsl {

}
