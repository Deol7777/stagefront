package com.ticketing.order.invariant;

import com.ticketing.order.domain.OrderEntity;
import com.ticketing.order.domain.OrderRepository;
import com.ticketing.order.domain.OrderStatus;
import com.ticketing.order.invariant.InvariantChecker.PaymentView;
import com.ticketing.order.invariant.InvariantChecker.SeatView;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for the cross-service invariants. The state reader is stubbed, so these
 * exercise the RULES rather than HTTP — the rules are where the reasoning lives.
 */
@ExtendWith(MockitoExtension.class)
class InvariantCheckerTest {

    @Mock OrderRepository orders;

    private static final Instant SETTLED = Instant.now().minus(Duration.ofMinutes(5));

    // ---- healthy system ---------------------------------------------------

    @Test
    void reports_consistent_when_all_services_agree() {
        var report = check(
                order("o1", OrderStatus.CONFIRMED, SETTLED),
                Map.of("o1", seat("seat-1", "SOLD", "o1")),
                Map.of("o1", List.of(payment("o1", "AUTHORIZED"))));

        assertTrue(report.consistent());
        assertEquals(1, report.ordersChecked());
    }

    // ---- the false-positive guard, the subtlest part ----------------------

    @Test
    void does_not_flag_a_saga_that_is_still_in_flight() {
        // Placed one second ago: no seat, no payment yet. This is a HEALTHY system
        // mid-saga, and flagging it would make the checker cry wolf on every order.
        var report = check(
                order("fresh", OrderStatus.PENDING, Instant.now()),
                Map.of(), Map.of());

        assertTrue(report.consistent(), "in-flight sagas must not count as violations");
        assertEquals(0, report.ordersChecked());
        assertEquals(1, report.ordersSkippedInFlight());
    }

    @Test
    void flags_an_order_still_pending_long_after_the_grace_period() {
        // Same shape as above, but old. Now it means the saga stalled — the event
        // was lost, parked in a DLQ, or a consumer is wedged.
        var report = check(
                order("stuck", OrderStatus.PENDING, SETTLED),
                Map.of(), Map.of());

        assertEquals(List.of(Violation.Rule.STUCK_PENDING_ORDER), rules(report));
    }

    // ---- confirmed-order invariants ---------------------------------------

    @Test
    void flags_confirmed_order_whose_seat_is_not_sold() {
        // We told the customer they own the seat; inventory disagrees.
        var report = check(
                order("o1", OrderStatus.CONFIRMED, SETTLED),
                Map.of(),   // inventory has no seat for this order at all
                Map.of("o1", List.of(payment("o1", "AUTHORIZED"))));

        assertEquals(List.of(Violation.Rule.CONFIRMED_ORDER_WITHOUT_SOLD_SEAT), rules(report));
    }

    @Test
    void flags_confirmed_order_with_no_authorized_payment() {
        // Seat handed over without taking the money.
        var report = check(
                order("o1", OrderStatus.CONFIRMED, SETTLED),
                Map.of("o1", seat("seat-1", "SOLD", "o1")),
                Map.of());

        assertEquals(List.of(Violation.Rule.CONFIRMED_ORDER_WITHOUT_AUTHORIZED_PAYMENT), rules(report));
    }

    // ---- compensation invariants ------------------------------------------

    @Test
    void flags_cancelled_order_that_still_holds_its_seat() {
        // Compensation failed to release: the seat is leaked and unsellable.
        var report = check(
                order("o1", OrderStatus.CANCELLED, SETTLED),
                Map.of("o1", seat("seat-1", "RESERVED", "o1")),
                Map.of());

        assertEquals(List.of(Violation.Rule.CANCELLED_ORDER_STILL_HOLDING_SEAT), rules(report));
    }

    @Test
    void flags_cancelled_order_that_was_charged_but_never_refunded() {
        // The expensive one: we kept the customer's money.
        var report = check(
                order("o1", OrderStatus.CANCELLED, SETTLED),
                Map.of(),
                Map.of("o1", List.of(payment("o1", "AUTHORIZED"))));

        assertEquals(List.of(Violation.Rule.CANCELLED_ORDER_WITH_UNREFUNDED_PAYMENT), rules(report));
    }

    @Test
    void accepts_cancelled_order_that_was_charged_and_refunded() {
        // Both rows exist — AUTHORIZED then REFUNDED is the correct end state for
        // a post-payment cancellation, not a violation.
        var report = check(
                order("o1", OrderStatus.CANCELLED, SETTLED),
                Map.of(),
                Map.of("o1", List.of(payment("o1", "AUTHORIZED"), payment("o1", "REFUNDED"))));

        assertTrue(report.consistent());
    }

    @Test
    void reports_several_violations_for_one_badly_broken_order() {
        // Confirmed, but neither the seat nor the money backs it up.
        var report = check(
                order("o1", OrderStatus.CONFIRMED, SETTLED),
                Map.of(), Map.of());

        assertFalse(report.consistent());
        assertEquals(2, report.violations().size());
    }

    // ---- helpers ----------------------------------------------------------

    private InvariantChecker.CheckReport check(OrderEntity order,
                                               Map<String, SeatView> seats,
                                               Map<String, List<PaymentView>> payments) {
        when(orders.findAll()).thenReturn(List.of(order));
        var reader = mock(InvariantChecker.ServiceStateReader.class);
        when(reader.seatsByReservingOrder()).thenReturn(seats);
        when(reader.paymentsByOrder()).thenReturn(payments);
        return new InvariantChecker(orders, reader).check(Duration.ofSeconds(30));
    }

    private List<Violation.Rule> rules(InvariantChecker.CheckReport report) {
        return report.violations().stream().map(Violation::rule).toList();
    }

    private OrderEntity order(String id, OrderStatus status, Instant createdAt) {
        OrderEntity o = mock(OrderEntity.class);
        // All lenient: the in-flight test skips the order before any of these are
        // read, and strict stubbing would fail that test for "unnecessary" stubs.
        lenient().when(o.getStatus()).thenReturn(status);
        lenient().when(o.getId()).thenReturn(id);
        lenient().when(o.getCreatedAt()).thenReturn(createdAt);
        lenient().when(o.getSeatId()).thenReturn("seat-1");
        return o;
    }

    private SeatView seat(String seatId, String status, String orderId) {
        return new SeatView(seatId, status, orderId);
    }

    private PaymentView payment(String orderId, String status) {
        return new PaymentView(orderId, status);
    }
}
