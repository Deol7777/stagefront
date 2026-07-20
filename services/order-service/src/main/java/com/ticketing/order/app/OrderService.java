package com.ticketing.order.app;

import com.ticketing.contracts.Money;
import com.ticketing.contracts.events.EventType;
import com.ticketing.contracts.events.OrderPlaced;
import com.ticketing.order.domain.OrderEntity;
import com.ticketing.order.domain.OrderRepository;
import com.ticketing.order.domain.OrderStatus;
import com.ticketing.order.outbox.OutboxWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.UUID;

/**
 * Application service for placing orders. Home of the transactional outbox write
 * and of the entry-point IDEMPOTENCY.
 *
 * <h2>Why idempotency lives here</h2>
 * Every consumer in this system dedups, because Kafka delivers at-least-once. The
 * HTTP door has the same problem for a different reason: a client that never
 * receives the response cannot tell "the order wasn't created" from "the response
 * was lost", so a correct client retries. Without dedup, a double-clicked button
 * or a proxy retry becomes a second real order. The client's {@code requestId} is
 * the dedup key — same idea as a consumer's, different door.
 */
@Service
public class OrderService {

    private static final Logger log = LoggerFactory.getLogger(OrderService.class);

    private final OrderRepository orders;
    private final OutboxWriter outbox;

    public OrderService(OrderRepository orders, OutboxWriter outbox) {
        this.orders = orders;
        this.outbox = outbox;
    }

    /**
     * Place an order, at most once per {@code requestId}.
     *
     * <p>The whole method is ONE transaction: the order row and the outbox row
     * commit together or not at all, and the advisory lock (taken below) is held
     * until that commit. We never write to Kafka here — publishing is the relay's
     * job, reading from the outbox. That separation is what prevents the
     * dual-write problem.
     *
     * @return the order's id — the same one for every retry of a given requestId
     */
    @Transactional
    public String placeOrder(PlaceOrderRequest req) {
        String requestId = req.requestId();

        if (requestId != null && !requestId.isBlank()) {
            // Serialize concurrent duplicates BEFORE looking, so the check-then-
            // insert below is race-free. A retry of an already-placed order finds
            // the committed row here and returns without inserting a second one.
            // Different requestIds don't contend (see OrderRepository.lockForRequest).
            orders.lockForRequest(requestId);

            var existing = orders.findByRequestId(requestId);
            if (existing.isPresent()) {
                log.info("Duplicate placeOrder for requestId={} — returning existing order {}",
                        requestId, existing.get().getId());
                return existing.get().getId();
            }
        }

        String orderId = UUID.randomUUID().toString();

        var order = new OrderEntity(
                orderId, req.userId(), req.seatId(), req.eventScheduleId(),
                req.amount(), req.currency(), OrderStatus.PENDING, Instant.now(),
                requestId);
        orders.save(order);

        var payload = new OrderPlaced(
                orderId, req.userId(), requestId,
                req.seatId(), req.eventScheduleId(),
                new Money(req.amount(), req.currency()));
        outbox.append(EventType.ORDER_PLACED, "Order", orderId, payload, /* traceId */ null);

        return orderId;
    }
}
