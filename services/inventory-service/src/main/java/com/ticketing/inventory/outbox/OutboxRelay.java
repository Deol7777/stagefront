package com.ticketing.inventory.outbox;

import io.micrometer.tracing.Span;
import io.micrometer.tracing.Tracer;
import io.micrometer.tracing.propagation.Propagator;
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
import java.util.Map;

/**
 * Polling publisher for the inventory outbox. Identical pattern to
 * order-service's relay (see docs/notes/04-transactional-outbox.md): block on
 * the Kafka ack, then mark published; at-least-once delivery.
 */
@Component
public class OutboxRelay {

    private static final Logger log = LoggerFactory.getLogger(OutboxRelay.class);

    private final OutboxRepository outbox;
    private final KafkaTemplate<String, String> kafka;
    private final Tracer tracer;
    private final Propagator propagator;
    private final int batchSize;

    public OutboxRelay(OutboxRepository outbox,
                       KafkaTemplate<String, String> kafka,
                       Tracer tracer,
                       Propagator propagator,
                       @Value("${outbox.relay.batch-size:100}") int batchSize) {
        this.outbox = outbox;
        this.kafka = kafka;
        this.tracer = tracer;
        this.propagator = propagator;
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
                publishInOriginatingTrace(row);
                row.markPublished(Instant.now());
            } catch (Exception e) {
                log.warn("Outbox publish failed for {} ({}); will retry next poll",
                        row.getId(), row.getEventType(), e);
                break;
            }
        }
    }

    /**
     * Publish inside the trace that originally produced the event, rebuilt from
     * the stored traceparent. Without this the scheduler thread's (empty) context
     * would be stamped on the record and the saga would fragment into unrelated
     * traces. Full explanation in order-service's OutboxRelay.
     */
    private void publishInOriginatingTrace(OutboxEntity row) throws Exception {
        String traceParent = row.getTraceParent();
        if (traceParent == null) {
            send(row);
            return;
        }

        Span span = propagator
                .extract(Map.of("traceparent", traceParent), Map::get)
                .name("outbox publish " + row.getEventType())
                .tag("messaging.destination", row.getTopic())
                .tag("outbox.id", row.getId().toString())
                .start();
        try (Tracer.SpanInScope scope = tracer.withSpan(span)) {
            send(row);
        } catch (Exception e) {
            span.error(e);
            throw e;
        } finally {
            span.end();
        }
    }

    private void send(OutboxEntity row) throws Exception {
        kafka.send(row.getTopic(), row.getPartitionKey(), row.getPayload()).get();
    }
}
