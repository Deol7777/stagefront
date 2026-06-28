package com.ticketing.payment.outbox;

import org.springframework.data.domain.Limit;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;
import java.util.UUID;

/** Repository for payment outbox rows. */
public interface OutboxRepository extends JpaRepository<OutboxEntity, UUID> {

    /** Oldest unpublished rows first, capped — the relay's poll query. */
    List<OutboxEntity> findByPublishedFalseOrderByCreatedAtAsc(Limit limit);
}
