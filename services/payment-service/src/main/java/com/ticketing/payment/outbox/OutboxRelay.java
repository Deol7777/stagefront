package com.ticketing.payment.outbox;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.domain.Limit;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.List;

/** Polling publisher for the payment outbox (same pattern; see note 04). */
@Component
public class OutboxRelay {

    private static final Logger log = LoggerFactory.getLogger(OutboxRelay.class);

    private final OutboxRepository outbox;
    private final KafkaTemplate<String, String> kafka;
    private final int batchSize;

    public OutboxRelay(OutboxRepository outbox,
                       KafkaTemplate<String, String> kafka,
                       @Value("${outbox.relay.batch-size:100}") int batchSize) {
        this.outbox = outbox;
        this.kafka = kafka;
        this.batchSize = batchSize;
    }

    @Scheduled(fixedDelayString = "${outbox.relay.poll-ms:1000}")
    @Transactional
    public void publishPending() {
        List<OutboxEntity> batch = outbox.findByPublishedFalseOrderByCreatedAtAsc(Limit.of(batchSize));
        if (batch.isEmpty()) {
            return;
        }
        for (OutboxEntity row : batch) {
            try {
                kafka.send(row.getTopic(), row.getPartitionKey(), row.getPayload()).get();
                row.markPublished(Instant.now());
            } catch (Exception e) {
                log.warn("Outbox publish failed for {} ({}); will retry next poll",
                        row.getId(), row.getEventType(), e);
                break;
            }
        }
    }
}
