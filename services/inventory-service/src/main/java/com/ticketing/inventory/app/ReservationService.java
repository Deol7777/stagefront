package com.ticketing.inventory.app;

import com.ticketing.contracts.EventEnvelope;
import com.ticketing.contracts.events.EventType;
import com.ticketing.contracts.events.OrderPlaced;
import com.ticketing.contracts.events.ReservationFailureReason;
import com.ticketing.contracts.events.SeatReservationFailed;
import com.ticketing.contracts.events.SeatReserved;
import com.ticketing.inventory.consumer.ProcessedEvent;
import com.ticketing.inventory.consumer.ProcessedEventRepository;
import com.ticketing.inventory.domain.SeatEntity;
import com.ticketing.inventory.domain.SeatRepository;
import com.ticketing.inventory.domain.SeatStatus;
import com.ticketing.inventory.outbox.OutboxWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import java.util.UUID;

/**
 * Core reservation logic. Handles an OrderPlaced event idempotently and either
 * reserves the seat (emitting SeatReserved) or fails (emitting
 * SeatReservationFailed). The whole method is one transaction so the seat
 * change, the dedup record, and the outbox event commit together.
 *
 * <p>The seat lock (Redis) is acquired by the caller (the listener) around this
 * call — this method assumes it holds exclusive access to the seat.
 */
@Service
public class ReservationService {

    private static final Logger log = LoggerFactory.getLogger(ReservationService.class);
    private static final String CONSUMER = "inventory-service";
    private static final Duration HOLD = Duration.ofMinutes(2);  // how long a reservation is held

    private final SeatRepository seats;
    private final ProcessedEventRepository processed;
    private final OutboxWriter outbox;

    public ReservationService(SeatRepository seats, ProcessedEventRepository processed, OutboxWriter outbox) {
        this.seats = seats;
        this.processed = processed;
        this.outbox = outbox;
    }

    @Transactional
    public void handle(EventEnvelope<OrderPlaced> envelope) {
        OrderPlaced order = envelope.payload();
        String dedupKey = order.dedupKey();   // = orderId

        // --- Idempotency gate: skip if we've already handled this event. ---
        if (processed.existsById(dedupKey)) {
            log.info("Duplicate OrderPlaced for order {} — skipping (idempotent)", order.orderId());
            return;
        }

        Optional<SeatEntity> seatOpt = seats.findById(order.seatId());
        String traceId = envelope.traceId();

        if (seatOpt.isEmpty()) {
            // Seat doesn't exist in inventory at all.
            fail(order, ReservationFailureReason.SOLD_OUT, traceId);
        } else {
            SeatEntity seat = seatOpt.get();
            if (seat.getStatus() != SeatStatus.AVAILABLE) {
                // Already held or sold by another order.
                fail(order, ReservationFailureReason.ALREADY_RESERVED, traceId);
            } else {
                reserve(seat, order, traceId);
            }
        }

        // Record that this event is handled — in the same transaction as the work.
        processed.save(new ProcessedEvent(dedupKey, CONSUMER, Instant.now()));
    }

    private void reserve(SeatEntity seat, OrderPlaced order, String traceId) {
        String reservationId = UUID.randomUUID().toString();
        Instant reservedUntil = Instant.now().plus(HOLD);
        seat.reserve(order.orderId(), reservationId, Instant.now());

        var event = new SeatReserved(
                order.orderId(), seat.getSeatId(), reservationId, reservedUntil, order.amount());
        outbox.append(EventType.SEAT_RESERVED, "Seat", seat.getSeatId(), event, traceId);
        log.info("Reserved seat {} for order {} (reservation {})",
                seat.getSeatId(), order.orderId(), reservationId);
    }

    private void fail(OrderPlaced order, ReservationFailureReason reason, String traceId) {
        var event = new SeatReservationFailed(order.orderId(), order.seatId(), reason);
        outbox.append(EventType.SEAT_RESERVATION_FAILED, "Seat", order.seatId(), event, traceId);
        log.info("Reservation failed for order {} seat {} — {}", order.orderId(), order.seatId(), reason);
    }
}
