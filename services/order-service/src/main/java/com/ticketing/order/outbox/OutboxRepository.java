package com.ticketing.order.outbox;

import org.springframework.data.domain.Limit;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;
import java.util.UUID;

/** Repository for outbox rows. */
public interface OutboxRepository extends JpaRepository<OutboxEntity, UUID> {

    /**
     * The relay's query: the oldest unpublished rows, capped by {@code limit}.
     * Oldest-first preserves event order; the cap bounds each poll's work.
     * (Backed by the partial index on unpublished rows.)
     */
    List<OutboxEntity> findByPublishedFalseOrderByCreatedAtAsc(Limit limit);
}
