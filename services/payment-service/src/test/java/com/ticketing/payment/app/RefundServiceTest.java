package com.ticketing.payment.app;

import com.ticketing.contracts.EventEnvelope;
import com.ticketing.contracts.events.EventType;
import com.ticketing.contracts.events.OrderCancelled;
import com.ticketing.contracts.events.PaymentRefunded;
import com.ticketing.payment.consumer.ProcessedEventRepository;
import com.ticketing.payment.domain.PaymentEntity;
import com.ticketing.payment.domain.PaymentRepository;
import com.ticketing.payment.domain.PaymentStatus;
import com.ticketing.payment.outbox.OutboxWriter;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.Optional;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Unit tests for refund compensation: refund only when an authorized payment exists. */
@ExtendWith(MockitoExtension.class)
class RefundServiceTest {

    @Mock PaymentRepository payments;
    @Mock ProcessedEventRepository processed;
    @Mock OutboxWriter outbox;

    private RefundService service() {
        return new RefundService(payments, processed, outbox);
    }

    private EventEnvelope<OrderCancelled> cancelled() {
        return EventType.ORDER_CANCELLED.newEnvelope(null,
                new OrderCancelled("order-1", "user-1", "seat-1", "USER_REQUESTED", Instant.now()));
    }

    @Test
    void refundsAnAuthorizedPayment() {
        when(processed.existsById("OrderCancelled#order-1")).thenReturn(false);
        PaymentEntity payment = mock(PaymentEntity.class);
        when(payment.getOrderId()).thenReturn("order-1");
        when(payment.getPaymentId()).thenReturn("pay-1");
        when(payment.getAmount()).thenReturn(new BigDecimal("80.00"));
        when(payment.getCurrency()).thenReturn("USD");
        when(payments.findFirstByOrderIdAndStatus("order-1", PaymentStatus.AUTHORIZED))
                .thenReturn(Optional.of(payment));

        service().onOrderCancelled(cancelled());

        verify(payment).markRefunded();
        verify(outbox).append(eq(EventType.PAYMENT_REFUNDED), eq("Payment"), eq("pay-1"),
                any(PaymentRefunded.class), any());
        verify(processed).save(any());
    }

    @Test
    void doesNothingWhenNoAuthorizedPayment() {
        // order never paid (declined / cancelled pre-payment) → no refund
        when(processed.existsById("OrderCancelled#order-1")).thenReturn(false);
        when(payments.findFirstByOrderIdAndStatus("order-1", PaymentStatus.AUTHORIZED))
                .thenReturn(Optional.empty());

        service().onOrderCancelled(cancelled());

        verify(outbox, never()).append(any(), any(), any(), any(), any());
        verify(processed).save(any());   // still marks the event handled
    }

    @Test
    void skipsDuplicateEvent() {
        when(processed.existsById("OrderCancelled#order-1")).thenReturn(true);

        service().onOrderCancelled(cancelled());

        verify(payments, never()).findFirstByOrderIdAndStatus(any(), any());
        verify(outbox, never()).append(any(), any(), any(), any(), any());
    }
}
