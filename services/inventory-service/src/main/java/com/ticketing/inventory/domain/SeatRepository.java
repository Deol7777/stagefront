package com.ticketing.inventory.domain;

import org.springframework.data.jpa.repository.JpaRepository;

/** CRUD for seats (keyed by seatId). */
public interface SeatRepository extends JpaRepository<SeatEntity, String> {
}
