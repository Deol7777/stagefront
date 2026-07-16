package com.ticketing.payment.app;

import com.ticketing.contracts.EventEnvelope;
import com.ticketing.contracts.Money;
import com.ticketing.contracts.events.EventType;
import com.ticketing.contracts.events.PaymentAuthorized;
import com.ticketing.contracts.events.PaymentDeclineReason;
import com.ticketing.contracts.events.PaymentDeclined;
import com.ticketing.contracts.events.SeatReserved;
import com.ticketing.payment.consumer.ProcessedEventRepository;
import com.ticketing.payment.domain.PaymentEntity;
import com.ticketing.payment.domain.PaymentRepository;
import com.ticketing.payment.domain.PaymentStatus;
import com.ticketing.payment.gateway.GatewayException;
import com.ticketing.payment.gateway.PaymentGatewayClient;
import com.ticketing.payment.outbox.OutboxWriter;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.math.BigDecimal;
import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

/**
 * Unit tests for the payment decision + idempotency logic. Repositories and the
 * outbox are mocked, so these run in-memory with no DB or Kafka.
 */
@ExtendWith(MockitoExtension.class)
class PaymentServiceTest {

    @Mock PaymentRepository payments;
    @Mock ProcessedEventRepository processed;
    @Mock OutboxWriter outbox;
    @Mock PaymentGatewayClient gateway;

    /** decline-above threshold = 1000 for all tests */
    private PaymentService service() {
        return new PaymentService(payments, processed, outbox, gateway, new BigDecimal("1000.00"));
    }

    private EventEnvelope<SeatReserved> seatReserved(String reservationId, String amount) {
        var payload = new SeatReserved("order-1", "seat-1", reservationId,
                Instant.now().plusSeconds(120), new Money(new BigDecimal(amount), "USD"));
        return EventType.SEAT_RESERVED.newEnvelope(null, payload);
    }

    @Test
    void authorizesWhenAmountAtOrBelowThreshold() {
        when(processed.existsById("res-1")).thenReturn(false);

        service().handle(seatReserved("res-1", "50.00"));

        // A payment row is saved as AUTHORIZED and a PaymentAuthorized event is queued.
        var saved = ArgumentCaptor.forClass(PaymentEntity.class);
        verify(payments).save(saved.capture());
        assertEquals(PaymentStatus.AUTHORIZED, saved.getValue().getStatus());
        verify(outbox).append(eq(EventType.PAYMENT_AUTHORIZED), eq("Payment"), any(),
                any(PaymentAuthorized.class), any());
        verify(processed).save(any());
    }

    @Test
    void declinesWhenAmountAboveThreshold() {
        when(processed.existsById("res-2")).thenReturn(false);

        service().handle(seatReserved("res-2", "5000.00"));

        var saved = ArgumentCaptor.forClass(PaymentEntity.class);
        verify(payments).save(saved.capture());
        assertEquals(PaymentStatus.DECLINED, saved.getValue().getStatus());
        verify(outbox).append(eq(EventType.PAYMENT_DECLINED), eq("Payment"), any(),
                any(PaymentDeclined.class), any());
        verify(processed).save(any());
    }

    @Test
    void declinesAsGatewayUnavailableWhenGatewayFails() {
        // Amount is within the business limit, so we reach the gateway — but the gateway
        // throws (breaker/retries exhausted). The charge must become a graceful decline
        // with reason GATEWAY_UNAVAILABLE, NOT an exception that dead-letters the message.
        when(processed.existsById("res-4")).thenReturn(false);
        when(gateway.charge(any(), any())).thenThrow(new GatewayException("down"));

        service().handle(seatReserved("res-4", "50.00"));

        var saved = ArgumentCaptor.forClass(PaymentEntity.class);
        verify(payments).save(saved.capture());
        assertEquals(PaymentStatus.DECLINED, saved.getValue().getStatus());

        var event = ArgumentCaptor.forClass(PaymentDeclined.class);
        verify(outbox).append(eq(EventType.PAYMENT_DECLINED), eq("Payment"), any(),
                event.capture(), any());
        assertEquals(PaymentDeclineReason.GATEWAY_UNAVAILABLE, event.getValue().reason());
        verify(processed).save(any());
    }

    @Test
    void skipsDuplicateEvent() {
        // dedup key already present → the whole handler must no-op
        when(processed.existsById("res-3")).thenReturn(true);

        service().handle(seatReserved("res-3", "50.00"));

        verifyNoInteractions(outbox);
        verify(payments, never()).save(any());
        verify(processed, never()).save(any());
    }
}
