package com.ticketing.inventory.consumer;

import org.springframework.data.jpa.repository.JpaRepository;

/** Dedup store. {@code existsById(dedupKey)} answers "have we handled this?" */
public interface ProcessedEventRepository extends JpaRepository<ProcessedEvent, String> {
}
