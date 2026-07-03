package com.ticketing.notification.consumer;

import org.springframework.data.jpa.repository.JpaRepository;

/** Dedup store. {@code existsById(namespacedKey)} answers "have we handled this?" */
public interface ProcessedEventRepository extends JpaRepository<ProcessedEvent, String> {
}
