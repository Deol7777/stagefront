package com.ticketing.order.app;

import com.ticketing.contracts.events.EventType;
import com.ticketing.order.domain.OrderEntity;
import com.ticketing.order.domain.OrderRepository;
import com.ticketing.order.outbox.OutboxWriter;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.math.BigDecimal;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Idempotency of the place-order entry point.
 *
 * <p>Kafka consumers dedup because delivery is at-least-once. HTTP has the same
 * property for a different reason: a client that loses the response cannot know
 * whether the order was created, so it retries. These tests pin that a retry
 * returns the SAME order rather than creating another, and that duplicates are
 * serialized by the advisory lock BEFORE the existence check.
 */
@ExtendWith(MockitoExtension.class)
class OrderServiceIdempotencyTest {

    @Mock OrderRepository orders;
    @Mock OutboxWriter outbox;

    @Test
    void creates_an_order_when_the_request_id_is_new() {
        when(orders.findByRequestId("req-1")).thenReturn(Optional.empty());

        String id = service().placeOrder(request("req-1"));

        // A fresh request writes the order and queues its event.
        verify(orders).save(any(OrderEntity.class));
        verify(outbox).append(eq(EventType.ORDER_PLACED), any(), eq(id), any(), any());
    }

    @Test
    void locks_before_it_checks_for_an_existing_order() {
        // The ordering IS the correctness argument: check-then-insert is only
        // race-free if the lock is already held when the check runs. Locking after
        // the check would leave exactly the window this fix is meant to close.
        when(orders.findByRequestId("req-1")).thenReturn(Optional.empty());

        service().placeOrder(request("req-1"));

        InOrder ordered = inOrder(orders);
        ordered.verify(orders).lockForRequest("req-1");
        ordered.verify(orders).findByRequestId("req-1");
        ordered.verify(orders).save(any(OrderEntity.class));
    }

    @Test
    void returns_the_existing_order_for_a_duplicate_request_without_inserting() {
        OrderEntity existing = order("order-1");
        when(orders.findByRequestId("req-1")).thenReturn(Optional.of(existing));

        assertEquals("order-1", service().placeOrder(request("req-1")));

        // The whole point: no second row, no second event.
        verify(orders, never()).save(any());
        verify(outbox, never()).append(any(), any(), any(), any(), any());
    }

    @Test
    void skips_the_lock_and_dedup_when_the_client_sends_no_request_id() {
        // Opting out is allowed; it just means no protection and no lock taken.
        String id = service().placeOrder(request(null));

        verify(orders, never()).lockForRequest(any());
        verify(orders, never()).findByRequestId(any());
        verify(orders).save(any(OrderEntity.class));
        assertEquals(36, id.length());   // a UUID was minted
    }

    private OrderService service() {
        return new OrderService(orders, outbox);
    }

    private PlaceOrderRequest request(String requestId) {
        return new PlaceOrderRequest("user-1", "seat-1", "show-1",
                new BigDecimal("50.00"), "USD", requestId);
    }

    private OrderEntity order(String id) {
        OrderEntity o = mock(OrderEntity.class);
        when(o.getId()).thenReturn(id);
        return o;
    }
}
