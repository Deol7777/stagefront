package com.ticketing.order.app;

import com.ticketing.contracts.EventEnvelope;
import com.ticketing.contracts.events.EventType;
import com.ticketing.contracts.events.OrderCancelled;
import com.ticketing.contracts.events.OrderConfirmed;
import com.ticketing.contracts.events.PaymentAuthorized;
import com.ticketing.contracts.events.PaymentDeclined;
import com.ticketing.contracts.events.SeatReservationFailed;
import com.ticketing.order.consumer.ProcessedEvent;
import com.ticketing.order.consumer.ProcessedEventRepository;
import com.ticketing.order.domain.OrderRepository;
import com.ticketing.order.domain.OrderStatus;
import com.ticketing.order.outbox.OutboxWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;

/**
 * Drives the order through the saga based on downstream outcomes. order-service
 * is the saga's "owner": it reacts to payment + reservation events and moves the
 * order to its terminal state, emitting OrderConfirmed or OrderCancelled.
 *
 * <p>Every handler is idempotent (namespaced dedup key) and transactional (order
 * status change + outbox event commit together). State transitions only happen
 * from PENDING — a second/duplicate outcome for an already-terminal order is a
 * no-op, which keeps the saga safe under at-least-once delivery.
 */
@Service
public class OrderSagaService {

    private static final Logger log = LoggerFactory.getLogger(OrderSagaService.class);
    private static final String CONSUMER = "order-service";

    private final OrderRepository orders;
    private final ProcessedEventRepository processed;
    private final OutboxWriter outbox;

    public OrderSagaService(OrderRepository orders, ProcessedEventRepository processed, OutboxWriter outbox) {
        this.orders = orders;
        this.processed = processed;
        this.outbox = outbox;
    }

    /** Payment succeeded → confirm the order. */
    @Transactional
    public void onPaymentAuthorized(EventEnvelope<PaymentAuthorized> env) {
        if (alreadyHandled(env.eventType(), env.payload().dedupKey())) return;
        PaymentAuthorized p = env.payload();

        orders.findById(p.orderId()).ifPresent(order -> {
            if (order.getStatus() == OrderStatus.PENDING) {
                order.setStatus(OrderStatus.CONFIRMED);
                var event = new OrderConfirmed(
                        order.getId(), order.getUserId(), order.getSeatId(), p.paymentId(), Instant.now());
                outbox.append(EventType.ORDER_CONFIRMED, "Order", order.getId(), event, env.traceId());
                log.info("Order {} CONFIRMED (payment {})", order.getId(), p.paymentId());
            }
        });
        record(env.eventType(), p.dedupKey());
    }

    /** Payment declined → cancel the order (compensation continues downstream). */
    @Transactional
    public void onPaymentDeclined(EventEnvelope<PaymentDeclined> env) {
        if (alreadyHandled(env.eventType(), env.payload().dedupKey())) return;
        PaymentDeclined p = env.payload();
        cancel(p.orderId(), "PAYMENT_DECLINED:" + p.reason(), env.traceId());
        record(env.eventType(), p.dedupKey());
    }

    /** Seat couldn't be reserved → cancel the order. Nothing to compensate. */
    @Transactional
    public void onSeatReservationFailed(EventEnvelope<SeatReservationFailed> env) {
        if (alreadyHandled(env.eventType(), env.payload().dedupKey())) return;
        SeatReservationFailed p = env.payload();
        cancel(p.orderId(), "SEAT_RESERVATION_FAILED:" + p.reason(), env.traceId());
        record(env.eventType(), p.dedupKey());
    }

    /** Move a PENDING order to CANCELLED and emit OrderCancelled. */
    private void cancel(String orderId, String reason, String traceId) {
        orders.findById(orderId).ifPresent(order -> {
            if (order.getStatus() == OrderStatus.PENDING) {
                order.setStatus(OrderStatus.CANCELLED);
                var event = new OrderCancelled(
                        order.getId(), order.getUserId(), order.getSeatId(), reason, Instant.now());
                outbox.append(EventType.ORDER_CANCELLED, "Order", order.getId(), event, traceId);
                log.info("Order {} CANCELLED ({})", order.getId(), reason);
            }
        });
    }

    /**
     * API-triggered cancellation. Unlike the saga's automatic cancel, this also
     * cancels an already-CONFIRMED (paid) order — which triggers the refund
     * compensation downstream (payment-service refunds, inventory releases the
     * sold seat). A command, not an event, so no dedup; the status guard makes it
     * safe to call twice.
     *
     * @return true if the order moved to CANCELLED, false if not cancellable
     */
    @Transactional
    public boolean requestCancel(String orderId, String reason) {
        return orders.findById(orderId).map(order -> {
            if (order.getStatus() == OrderStatus.CANCELLED) {
                return false;   // already cancelled — nothing to do
            }
            order.setStatus(OrderStatus.CANCELLED);
            var event = new OrderCancelled(
                    order.getId(), order.getUserId(), order.getSeatId(), reason, Instant.now());
            outbox.append(EventType.ORDER_CANCELLED, "Order", order.getId(), event, null);
            log.info("Order {} CANCELLED on request ({})", order.getId(), reason);
            return true;
        }).orElse(false);
    }

    // --- idempotency helpers (namespaced so different event types don't collide) ---

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
