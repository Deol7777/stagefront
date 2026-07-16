package com.ticketing.inventory.app;

import com.ticketing.contracts.EventEnvelope;
import com.ticketing.contracts.events.OrderCancelled;
import com.ticketing.contracts.events.OrderConfirmed;
import com.ticketing.contracts.events.PaymentDeclined;
import com.ticketing.inventory.cache.SeatCache;
import com.ticketing.inventory.consumer.ProcessedEvent;
import com.ticketing.inventory.consumer.ProcessedEventRepository;
import com.ticketing.inventory.domain.SeatEntity;
import com.ticketing.inventory.domain.SeatStatus;
import com.ticketing.inventory.domain.SeatRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;

/**
 * The compensation + finalization side of inventory. Reacts to saga outcomes:
 * <ul>
 *   <li>{@code PaymentDeclined} → <b>release</b> the seat the order held (compensation)</li>
 *   <li>{@code OrderCancelled} → <b>release</b> the seat if still held (compensation,
 *       covers other cancel reasons too)</li>
 *   <li>{@code OrderConfirmed} → <b>mark the seat SOLD</b> (finalization)</li>
 * </ul>
 *
 * <p>All handlers are idempotent and transactional, and every seat change is
 * guarded by the seat's current status so a duplicate (or a release arriving from
 * both PaymentDeclined and OrderCancelled) is a safe no-op.
 *
 * <p>No Redis lock here: these act on a seat already tied to one order, so there
 * is no cross-order contention to guard.
 */
@Service
public class InventorySagaService {

    private static final Logger log = LoggerFactory.getLogger(InventorySagaService.class);
    private static final String CONSUMER = "inventory-service";

    private final SeatRepository seats;
    private final ProcessedEventRepository processed;
    private final SeatCache cache;

    public InventorySagaService(SeatRepository seats, ProcessedEventRepository processed, SeatCache cache) {
        this.seats = seats;
        this.processed = processed;
        this.cache = cache;
    }

    /** Compensation: payment failed → free the seat this order was holding. */
    @Transactional
    public void onPaymentDeclined(EventEnvelope<PaymentDeclined> env) {
        if (alreadyHandled(env.eventType(), env.payload().dedupKey())) return;
        String orderId = env.payload().orderId();
        // PaymentDeclined has no seatId; find the seat by who holds it.
        seats.findByReservedByOrder(orderId).ifPresent(seat -> releaseIfHeld(seat, orderId));
        record(env.eventType(), env.payload().dedupKey());
    }

    /** Compensation: order cancelled → release the seat if still held by it. */
    @Transactional
    public void onOrderCancelled(EventEnvelope<OrderCancelled> env) {
        if (alreadyHandled(env.eventType(), env.payload().dedupKey())) return;
        OrderCancelled c = env.payload();
        seats.findById(c.seatId()).ifPresent(seat -> releaseIfHeld(seat, c.orderId()));
        record(env.eventType(), c.dedupKey());
    }

    /** Finalization: order confirmed → the seat is permanently sold. */
    @Transactional
    public void onOrderConfirmed(EventEnvelope<OrderConfirmed> env) {
        if (alreadyHandled(env.eventType(), env.payload().dedupKey())) return;
        OrderConfirmed c = env.payload();
        seats.findById(c.seatId()).ifPresent(seat -> {
            if (seat.getStatus() == SeatStatus.RESERVED && c.orderId().equals(seat.getReservedByOrder())) {
                seat.markSold(Instant.now());
                cache.evictAfterCommit(seat.getSeatId());   // SOLD → invalidate cached view
                log.info("Seat {} SOLD (order {} confirmed)", seat.getSeatId(), c.orderId());
            }
        });
        record(env.eventType(), c.dedupKey());
    }

    /**
     * Release the seat if this order holds it. Covers both RESERVED (cancelled
     * before confirmation) and SOLD (cancelled after payment → refund path), so a
     * post-confirmation cancellation frees the sold seat back to AVAILABLE.
     * Status + owner guarded, so duplicates/replays are no-ops.
     */
    private void releaseIfHeld(SeatEntity seat, String orderId) {
        boolean heldByOrder = orderId.equals(seat.getReservedByOrder());
        boolean releasable = seat.getStatus() == SeatStatus.RESERVED || seat.getStatus() == SeatStatus.SOLD;
        if (heldByOrder && releasable) {
            seat.release(Instant.now());
            cache.evictAfterCommit(seat.getSeatId());   // back to AVAILABLE → invalidate cached view
            log.info("Seat {} RELEASED (order {} compensation)", seat.getSeatId(), orderId);
        }
    }

    private boolean alreadyHandled(String eventType, String dedupKey) {
        String key = eventType + "#" + dedupKey;
        if (processed.existsById(key)) {
            log.info("Duplicate {} — skipping (idempotent)", key);
            return true;
        }
        return false;
    }

    private void record(String eventType, String dedupKey) {
        processed.save(new ProcessedEvent(eventType + "#" + dedupKey, CONSUMER, Instant.now()));
    }
}
