package com.ticketing.order.outbox;

import io.micrometer.tracing.Span;
import io.micrometer.tracing.Tracer;
import io.micrometer.tracing.propagation.Propagator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.domain.Limit;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.List;
import java.util.Map;

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
    private final Tracer tracer;
    private final Propagator propagator;
    private final int batchSize;

    public OutboxRelay(OutboxRepository outbox,
                       KafkaTemplate<String, String> kafka,
                       Tracer tracer,
                       Propagator propagator,
                       @org.springframework.beans.factory.annotation.Value("${outbox.relay.batch-size:100}") int batchSize) {
        this.outbox = outbox;
        this.kafka = kafka;
        this.tracer = tracer;
        this.propagator = propagator;
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
                publishInOriginatingTrace(row);
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

    /**
     * Publish one row inside the trace that ORIGINALLY produced the event.
     *
     * <p>This method is the fix for the thing that quietly breaks tracing in every
     * outbox implementation. The relay runs on a scheduler thread: at this point
     * the HTTP request (or upstream listener) that created the event finished
     * seconds ago and its trace context is long gone. If we just called send(),
     * the KafkaTemplate would stamp the record with the SCHEDULER's context, so
     * every consumer downstream would attach itself to "the 09:41:03 outbox poll"
     * rather than to the order. One saga would show up in Jaeger as a handful of
     * unrelated single-span traces — which is exactly what happened before this.
     *
     * <p>So we rebuild the original context from the stored traceparent:
     * {@code propagator.extract} parses the header into a REMOTE PARENT, and the
     * span we start becomes a child of the original saga span even though nothing
     * in this JVM's thread state remembers it. Opening a scope makes it current,
     * so the KafkaTemplate's own producer observation injects the restored context
     * into the outgoing record's headers. The consumer extracts it on the far side
     * and the chain continues.
     *
     * <p>Rows written before this column existed (or produced with tracing off)
     * have a null traceparent — those publish untraced rather than failing.
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

    /**
     * Block on the send ack: only mark published once Kafka confirms.
     * Key = partitionKey (orderId) → per-order ordering on the topic.
     */
    private void send(OutboxEntity row) throws Exception {
        kafka.send(row.getTopic(), row.getPartitionKey(), row.getPayload()).get();
    }
}
