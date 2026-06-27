package com.ticketing.inventory.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.ticketing.contracts.EventEnvelope;
import com.ticketing.contracts.Topics;
import com.ticketing.contracts.events.OrderPlaced;
import com.ticketing.inventory.app.ReservationService;
import com.ticketing.inventory.lock.RedisSeatLock;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.time.Duration;

/**
 * Kafka consumer for {@code orders.placed}. For each event it:
 * <ol>
 *   <li>parses the envelope (a bad/poison message throws → retried → DLQ),</li>
 *   <li>acquires the Redis seat lock (mutual exclusion across instances),</li>
 *   <li>delegates to {@link ReservationService} (idempotent + transactional),</li>
 *   <li>always releases the lock.</li>
 * </ol>
 *
 * <p>If the listener throws, Spring's error handler retries with backoff and, on
 * exhaustion, publishes the record to {@code orders.placed.DLQ} (configured in
 * {@link com.ticketing.inventory.config.KafkaConsumerConfig}).
 */
@Component
public class OrderPlacedListener {

    private final ObjectMapper mapper;
    private final ReservationService reservationService;
    private final RedisSeatLock lock;
    private final Duration lockTtl;

    public OrderPlacedListener(ObjectMapper mapper,
                               ReservationService reservationService,
                               RedisSeatLock lock,
                               @Value("${seat.lock.ttl-ms:5000}") long lockTtlMs) {
        this.mapper = mapper;
        this.reservationService = reservationService;
        this.lock = lock;
        this.lockTtl = Duration.ofMillis(lockTtlMs);
    }

    @KafkaListener(topics = Topics.ORDERS_PLACED, groupId = "${spring.kafka.consumer.group-id}")
    public void onOrderPlaced(String value) throws Exception {
        EventEnvelope<OrderPlaced> envelope = parse(value);
        String seatId = envelope.payload().seatId();
        String lockKey = "seat-lock:" + seatId;

        String token = lock.tryAcquire(lockKey, lockTtl);
        if (token == null) {
            // Another worker is mid-reservation for this seat. Throw so the
            // record is retried shortly (the holder finishes in milliseconds).
            throw new IllegalStateException("Seat " + seatId + " is locked; will retry");
        }
        try {
            reservationService.handle(envelope);
        } finally {
            lock.release(lockKey, token);
        }
    }

    /** Deserialize the JSON value into a typed envelope. Failure → poison → DLQ. */
    private EventEnvelope<OrderPlaced> parse(String value) throws Exception {
        return mapper.readValue(
                value,
                mapper.getTypeFactory().constructParametricType(EventEnvelope.class, OrderPlaced.class));
    }
}
