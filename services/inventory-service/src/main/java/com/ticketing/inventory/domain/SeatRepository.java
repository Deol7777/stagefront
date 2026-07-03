package com.ticketing.inventory.domain;

import org.springframework.data.jpa.repository.JpaRepository;

import java.util.Optional;

/** CRUD for seats (keyed by seatId). */
public interface SeatRepository extends JpaRepository<SeatEntity, String> {

    /** Find the seat currently held by an order (PaymentDeclined carries no seatId). */
    Optional<SeatEntity> findByReservedByOrder(String orderId);
}
