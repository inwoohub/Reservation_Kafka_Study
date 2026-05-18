package com.example.reservationhistoryconsumer.domain.reservation;

import static com.querydsl.core.types.PathMetadataFactory.*;

import com.querydsl.core.types.dsl.*;

import com.querydsl.core.types.PathMetadata;
import javax.annotation.processing.Generated;
import com.querydsl.core.types.Path;


/**
 * QReservation is a Querydsl query type for Reservation
 */
@Generated("com.querydsl.codegen.DefaultEntitySerializer")
public class QReservation extends EntityPathBase<Reservation> {

    private static final long serialVersionUID = -1443188385L;

    public static final QReservation reservation = new QReservation("reservation");

    public final StringPath birthDate = createString("birthDate");

    public final StringPath buyerName = createString("buyerName");

    public final DateTimePath<java.time.LocalDateTime> createdAt = createDateTime("createdAt", java.time.LocalDateTime.class);

    public final StringPath eventId = createString("eventId");

    public final NumberPath<Long> id = createNumber("id", Long.class);

    public final StringPath orderId = createString("orderId");

    public final NumberPath<Long> productId = createNumber("productId", Long.class);

    public final NumberPath<Integer> quantity = createNumber("quantity", Integer.class);

    public final EnumPath<ReservationStatus> reservationStatus = createEnum("reservationStatus", ReservationStatus.class);

    public final StringPath tempPassword = createString("tempPassword");

    public final NumberPath<Long> timestamp = createNumber("timestamp", Long.class);

    public final NumberPath<Integer> totalPrice = createNumber("totalPrice", Integer.class);

    public QReservation(String variable) {
        super(Reservation.class, forVariable(variable));
    }

    public QReservation(Path<? extends Reservation> path) {
        super(path.getType(), path.getMetadata());
    }

    public QReservation(PathMetadata metadata) {
        super(Reservation.class, metadata);
    }

}

