package com.ticketing.inventory.app;

import com.ticketing.contracts.EventEnvelope;
import com.ticketing.contracts.Money;
import com.ticketing.contracts.events.EventType;
import com.ticketing.contracts.events.OrderPlaced;
import com.ticketing.contracts.events.SeatReservationFailed;
import com.ticketing.contracts.events.SeatReserved;
import com.ticketing.inventory.consumer.ProcessedEventRepository;
import com.ticketing.inventory.domain.SeatEntity;
import com.ticketing.inventory.domain.SeatRepository;
import com.ticketing.inventory.domain.SeatStatus;
import com.ticketing.inventory.outbox.OutboxWriter;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.math.BigDecimal;
import java.util.Optional;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

/**
 * Unit tests for seat-reservation logic. The seat entity itself is mocked so we
 * can drive its status without needing a DB. The Redis lock is not involved here
 * (it's held by the listener, outside this service).
 */
@ExtendWith(MockitoExtension.class)
class ReservationServiceTest {

    @Mock SeatRepository seats;
    @Mock ProcessedEventRepository processed;
    @Mock OutboxWriter outbox;

    private ReservationService service() {
        return new ReservationService(seats, processed, outbox);
    }

    private EventEnvelope<OrderPlaced> orderPlaced() {
        var payload = new OrderPlaced("order-1", "user-1", "req-1", "seat-1", "show-9",
                new Money(new BigDecimal("50.00"), "USD"));
        return EventType.ORDER_PLACED.newEnvelope(null, payload);
    }

    @Test
    void reservesAnAvailableSeat() {
        when(processed.existsById("OrderPlaced#order-1")).thenReturn(false);
        SeatEntity seat = org.mockito.Mockito.mock(SeatEntity.class);
        when(seat.getSeatId()).thenReturn("seat-1");
        when(seat.getStatus()).thenReturn(SeatStatus.AVAILABLE);
        when(seats.findById("seat-1")).thenReturn(Optional.of(seat));

        service().handle(orderPlaced());

        verify(seat).reserve(eq("order-1"), anyString(), any());
        verify(outbox).append(eq(EventType.SEAT_RESERVED), eq("Seat"), eq("seat-1"),
                any(SeatReserved.class), any());
        verify(processed).save(any());
    }

    @Test
    void failsWhenSeatAlreadyReserved() {
        when(processed.existsById("OrderPlaced#order-1")).thenReturn(false);
        SeatEntity seat = org.mockito.Mockito.mock(SeatEntity.class);
        when(seat.getStatus()).thenReturn(SeatStatus.RESERVED);
        when(seats.findById("seat-1")).thenReturn(Optional.of(seat));

        service().handle(orderPlaced());

        verify(seat, never()).reserve(any(), any(), any());
        verify(outbox).append(eq(EventType.SEAT_RESERVATION_FAILED), eq("Seat"), eq("seat-1"),
                any(SeatReservationFailed.class), any());
        verify(processed).save(any());
    }

    @Test
    void failsWhenSeatDoesNotExist() {
        when(processed.existsById("OrderPlaced#order-1")).thenReturn(false);
        when(seats.findById("seat-1")).thenReturn(Optional.empty());

        service().handle(orderPlaced());

        verify(outbox).append(eq(EventType.SEAT_RESERVATION_FAILED), eq("Seat"), eq("seat-1"),
                any(SeatReservationFailed.class), any());
        verify(processed).save(any());
    }

    @Test
    void skipsDuplicateEvent() {
        when(processed.existsById("OrderPlaced#order-1")).thenReturn(true);

        service().handle(orderPlaced());

        verifyNoInteractions(outbox);
        verify(seats, never()).findById(any());
        verify(processed, never()).save(any());
    }
}
