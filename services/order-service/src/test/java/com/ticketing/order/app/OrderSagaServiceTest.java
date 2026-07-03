package com.ticketing.order.app;

import com.ticketing.contracts.EventEnvelope;
import com.ticketing.contracts.Money;
import com.ticketing.contracts.events.EventType;
import com.ticketing.contracts.events.OrderCancelled;
import com.ticketing.contracts.events.OrderConfirmed;
import com.ticketing.contracts.events.PaymentAuthorized;
import com.ticketing.contracts.events.PaymentDeclineReason;
import com.ticketing.contracts.events.PaymentDeclined;
import com.ticketing.order.consumer.ProcessedEventRepository;
import com.ticketing.order.domain.OrderEntity;
import com.ticketing.order.domain.OrderRepository;
import com.ticketing.order.domain.OrderStatus;
import com.ticketing.order.outbox.OutboxWriter;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

/**
 * Unit tests for the order saga: confirm/cancel transitions, the status guards
 * that make them idempotent, and the API-triggered cancel that drives refunds.
 */
@ExtendWith(MockitoExtension.class)
class OrderSagaServiceTest {

    @Mock OrderRepository orders;
    @Mock ProcessedEventRepository processed;
    @Mock OutboxWriter outbox;

    private OrderSagaService service() {
        return new OrderSagaService(orders, processed, outbox);
    }

    private OrderEntity order(OrderStatus status) {
        OrderEntity o = mock(OrderEntity.class);
        when(o.getStatus()).thenReturn(status);
        // Used only on the paths that emit an event — lenient so the no-op /
        // guard tests (which never read them) don't trip strict stubbing.
        lenient().when(o.getId()).thenReturn("order-1");
        lenient().when(o.getUserId()).thenReturn("user-1");
        lenient().when(o.getSeatId()).thenReturn("seat-1");
        return o;
    }

    private EventEnvelope<PaymentAuthorized> authorized() {
        return EventType.PAYMENT_AUTHORIZED.newEnvelope(null,
                new PaymentAuthorized("order-1", "pay-1",
                        new Money(new BigDecimal("50.00"), "USD"), Instant.now()));
    }

    private EventEnvelope<PaymentDeclined> declined() {
        return EventType.PAYMENT_DECLINED.newEnvelope(null,
                new PaymentDeclined("order-1", "pay-1", PaymentDeclineReason.INSUFFICIENT_FUNDS));
    }

    @Test
    void confirmsPendingOrderOnAuthorized() {
        when(processed.existsById("PaymentAuthorized#pay-1")).thenReturn(false);
        OrderEntity o = order(OrderStatus.PENDING);
        when(orders.findById("order-1")).thenReturn(Optional.of(o));

        service().onPaymentAuthorized(authorized());

        verify(o).setStatus(OrderStatus.CONFIRMED);
        verify(outbox).append(eq(EventType.ORDER_CONFIRMED), eq("Order"), eq("order-1"),
                any(OrderConfirmed.class), any());
        verify(processed).save(any());
    }

    @Test
    void ignoresAuthorizedWhenOrderNotPending() {
        // guard: an already-terminal order is not re-confirmed (still records dedup)
        when(processed.existsById("PaymentAuthorized#pay-1")).thenReturn(false);
        OrderEntity o = order(OrderStatus.CANCELLED);
        when(orders.findById("order-1")).thenReturn(Optional.of(o));

        service().onPaymentAuthorized(authorized());

        verify(o, never()).setStatus(any());
        verify(outbox, never()).append(any(), any(), any(), any(), any());
        verify(processed).save(any());
    }

    @Test
    void cancelsPendingOrderOnDeclined() {
        when(processed.existsById("PaymentDeclined#pay-1")).thenReturn(false);
        OrderEntity o = order(OrderStatus.PENDING);
        when(orders.findById("order-1")).thenReturn(Optional.of(o));

        service().onPaymentDeclined(declined());

        verify(o).setStatus(OrderStatus.CANCELLED);
        verify(outbox).append(eq(EventType.ORDER_CANCELLED), eq("Order"), eq("order-1"),
                any(OrderCancelled.class), any());
    }

    @Test
    void skipsDuplicateEvent() {
        when(processed.existsById("PaymentAuthorized#pay-1")).thenReturn(true);

        service().onPaymentAuthorized(authorized());

        verifyNoInteractions(outbox);
        verify(orders, never()).findById(any());
        verify(processed, never()).save(any());
    }

    @Test
    void requestCancelCancelsConfirmedOrder() {
        // API cancel of a paid order → triggers refund downstream
        OrderEntity o = order(OrderStatus.CONFIRMED);
        when(orders.findById("order-1")).thenReturn(Optional.of(o));

        boolean result = service().requestCancel("order-1", "USER_REQUESTED");

        assertTrue(result);
        verify(o).setStatus(OrderStatus.CANCELLED);
        verify(outbox).append(eq(EventType.ORDER_CANCELLED), eq("Order"), eq("order-1"),
                any(OrderCancelled.class), any());
    }

    @Test
    void requestCancelIsNoOpWhenAlreadyCancelled() {
        OrderEntity o = order(OrderStatus.CANCELLED);
        when(orders.findById("order-1")).thenReturn(Optional.of(o));

        boolean result = service().requestCancel("order-1", "USER_REQUESTED");

        assertFalse(result);
        verify(o, never()).setStatus(any());
        verifyNoInteractions(outbox);
    }
}
