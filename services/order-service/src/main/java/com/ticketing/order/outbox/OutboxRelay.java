package com.ticketing.order.outbox;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.domain.Limit;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.List;

/**
 * The outbox relay (a "polling publisher"): on a timer it reads unpublished
 * outbox rows and pushes each to Kafka, then marks it published.
 *
 * <p>Why a relay instead of publishing inline when the order is saved? Because
 * publishing inline would be a dual write (DB + Kafka in one breath) — if the
 * app crashed between them, they'd disagree. Storing the event in the DB first
 * (atomic with the business write) and publishing later guarantees every
 * committed order eventually produces its event.
 *
 * <p>Delivery is AT-LEAST-ONCE: if Kafka accepts a message but this process dies
 * before marking the row published, the next poll re-sends it. That's fine and
 * expected — consumers are idempotent (dedup key), so a duplicate is a no-op.
 */
@Component
public class OutboxRelay {

    private static final Logger log = LoggerFactory.getLogger(OutboxRelay.class);

    private final OutboxRepository outbox;
    private final KafkaTemplate<String, String> kafka;
    private final int batchSize;

    public OutboxRelay(OutboxRepository outbox,
                       KafkaTemplate<String, String> kafka,
                       @org.springframework.beans.factory.annotation.Value("${outbox.relay.batch-size:100}") int batchSize) {
        this.outbox = outbox;
        this.kafka = kafka;
        this.batchSize = batchSize;
    }

    /**
     * Poll + publish. Runs every {@code outbox.relay.poll-ms} after the previous
     * run finishes. Transactional so the "mark published" updates commit together.
     */
    @Scheduled(fixedDelayString = "${outbox.relay.poll-ms:1000}")
    @Transactional
    public void publishPending() {
        List<OutboxEntity> batch = outbox.findByPublishedFalseOrderByCreatedAtAsc(Limit.of(batchSize));
        if (batch.isEmpty()) {
            return;
        }
        for (OutboxEntity row : batch) {
            try {
                // Block on the send ack: only mark published once Kafka confirms.
                // Key = partitionKey (orderId) → per-order ordering on the topic.
                kafka.send(row.getTopic(), row.getPartitionKey(), row.getPayload()).get();
                row.markPublished(Instant.now());
            } catch (Exception e) {
                // Leave the row unpublished; the next poll retries it. Stop this
                // batch here so we don't reorder events for the same key.
                log.warn("Outbox publish failed for {} ({}); will retry next poll",
                        row.getId(), row.getEventType(), e);
                break;
            }
        }
    }
}
