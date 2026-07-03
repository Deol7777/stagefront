package com.ticketing.inventory.app;

import com.ticketing.contracts.EventEnvelope;
import com.ticketing.contracts.events.EventType;
import com.ticketing.contracts.events.OrderCancelled;
import com.ticketing.contracts.events.OrderConfirmed;
import com.ticketing.contracts.events.PaymentDeclineReason;
import com.ticketing.contracts.events.PaymentDeclined;
import com.ticketing.inventory.consumer.ProcessedEventRepository;
import com.ticketing.inventory.domain.SeatEntity;
import com.ticketing.inventory.domain.SeatRepository;
import com.ticketing.inventory.domain.SeatStatus;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Instant;
import java.util.Optional;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for the compensation + finalization logic (release / mark sold),
 * including the status + owner guards that make duplicates safe no-ops.
 */
@ExtendWith(MockitoExtension.class)
class InventorySagaServiceTest {

    @Mock SeatRepository seats;
    @Mock ProcessedEventRepository processed;

    private InventorySagaService service() {
        return new InventorySagaService(seats, processed);
    }

    private SeatEntity seat(SeatStatus status, String owner) {
        SeatEntity s = mock(SeatEntity.class);
        when(s.getStatus()).thenReturn(status);
        when(s.getReservedByOrder()).thenReturn(owner);
        return s;
    }

    private EventEnvelope<OrderConfirmed> confirmed() {
        return EventType.ORDER_CONFIRMED.newEnvelope(null,
                new OrderConfirmed("order-1", "user-1", "seat-1", "pay-1", Instant.now()));
    }

    private EventEnvelope<OrderCancelled> cancelled() {
        return EventType.ORDER_CANCELLED.newEnvelope(null,
                new OrderCancelled("order-1", "user-1", "seat-1", "USER_REQUESTED", Instant.now()));
    }

    private EventEnvelope<PaymentDeclined> declined() {
        return EventType.PAYMENT_DECLINED.newEnvelope(null,
                new PaymentDeclined("order-1", "pay-1", PaymentDeclineReason.INSUFFICIENT_FUNDS));
    }

    @Test
    void marksSeatSoldOnConfirm() {
        when(processed.existsById("OrderConfirmed#order-1")).thenReturn(false);
        SeatEntity s = seat(SeatStatus.RESERVED, "order-1");
        when(seats.findById("seat-1")).thenReturn(Optional.of(s));

        service().onOrderConfirmed(confirmed());

        verify(s).markSold(any());
        verify(processed).save(any());
    }

    @Test
    void releasesReservedSeatOnCancel() {
        when(processed.existsById("OrderCancelled#order-1")).thenReturn(false);
        SeatEntity s = seat(SeatStatus.RESERVED, "order-1");
        when(seats.findById("seat-1")).thenReturn(Optional.of(s));

        service().onOrderCancelled(cancelled());

        verify(s).release(any());
    }

    @Test
    void releasesSoldSeatOnCancel() {
        // post-payment cancellation (refund path): the seat is SOLD, must free it
        when(processed.existsById("OrderCancelled#order-1")).thenReturn(false);
        SeatEntity s = seat(SeatStatus.SOLD, "order-1");
        when(seats.findById("seat-1")).thenReturn(Optional.of(s));

        service().onOrderCancelled(cancelled());

        verify(s).release(any());
    }

    @Test
    void releasesSeatOnPaymentDeclined() {
        // PaymentDeclined has no seatId → looked up by who holds it
        when(processed.existsById("PaymentDeclined#pay-1")).thenReturn(false);
        SeatEntity s = seat(SeatStatus.RESERVED, "order-1");
        when(seats.findByReservedByOrder("order-1")).thenReturn(Optional.of(s));

        service().onPaymentDeclined(declined());

        verify(s).release(any());
    }

    @Test
    void doesNotReleaseSeatHeldByAnotherOrder() {
        // guard: a stray cancel for order-1 must not free a seat owned by order-2
        when(processed.existsById("OrderCancelled#order-1")).thenReturn(false);
        SeatEntity s = seat(SeatStatus.RESERVED, "order-2");
        when(seats.findById("seat-1")).thenReturn(Optional.of(s));

        service().onOrderCancelled(cancelled());

        verify(s, never()).release(any());
    }

    @Test
    void skipsDuplicateConfirm() {
        when(processed.existsById("OrderConfirmed#order-1")).thenReturn(true);

        service().onOrderConfirmed(confirmed());

        verify(seats, never()).findById(any());
        verify(processed, never()).save(any());
    }
}
