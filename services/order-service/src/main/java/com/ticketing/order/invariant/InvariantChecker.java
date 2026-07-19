package com.ticketing.order.invariant;

import com.ticketing.order.domain.OrderEntity;
import com.ticketing.order.domain.OrderRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Cross-service invariant checker (a "reconciler").
 *
 * <h2>Why this exists</h2>
 * A saga gives us eventual consistency, not atomicity. There is no transaction
 * spanning the four databases, so nothing structurally prevents them from
 * disagreeing: a compensation whose event was lost, a consumer that crashed
 * between two writes, a message parked in a DLQ. Each service is individually
 * correct and the SYSTEM is still wrong.
 *
 * <p>The pattern's answer is not "prevent it" — you can't, without giving up the
 * availability the saga bought you. It is: <b>detect it out of band and say so
 * loudly.</b> That is what this class does. It reads each service's state through
 * its public API and asserts the statements that must be true once things settle.
 *
 * <h2>The subtlety: in-flight is not broken</h2>
 * A saga mid-flight looks EXACTLY like a violation. An order is PENDING with no
 * payment yet — that is not corruption, that is Tuesday. So every check is
 * gated on a {@code grace} period: only orders that stopped changing longer ago
 * than that are eligible. Too short and you cry wolf on healthy traffic; too long
 * and you find real breakage late. This trade-off is inherent to reconciling an
 * eventually-consistent system, not a shortcoming of this implementation.
 *
 * <h2>What this is NOT</h2>
 * The three reads are not a consistent snapshot — no distributed lock, no global
 * transaction. State can shift underneath us mid-scan. A reported violation is
 * therefore a STRONG SUSPICION, not a proof; the standard follow-up is to re-run
 * the check and see whether it persists. Real reconcilers treat a violation that
 * survives two passes as actionable.
 *
 * <p>It also only detects. Auto-repair is deliberately out of scope: a reconciler
 * that "fixes" state it misread turns a reporting bug into data loss.
 */
@Service
public class InvariantChecker {

    private static final Logger log = LoggerFactory.getLogger(InvariantChecker.class);

    /** Default settle time before an order is eligible to be judged. */
    static final Duration DEFAULT_GRACE = Duration.ofSeconds(30);

    private final OrderRepository orders;
    private final ServiceStateReader stateReader;

    public InvariantChecker(OrderRepository orders, ServiceStateReader stateReader) {
        this.orders = orders;
        this.stateReader = stateReader;
    }

    public CheckReport check() {
        return check(DEFAULT_GRACE);
    }

    /**
     * Run every invariant over every settled order.
     *
     * @param grace how long an order must have been untouched before we judge it
     */
    public CheckReport check(Duration grace) {
        // Fetch the other services' state ONCE up front rather than per order:
        // an order-at-a-time scan would be N+1 HTTP calls and would also widen the
        // window over which the snapshot smears.
        Map<String, SeatView> seatsByOrder = stateReader.seatsByReservingOrder();
        Map<String, List<PaymentView>> paymentsByOrder = stateReader.paymentsByOrder();

        Instant cutoff = Instant.now().minus(grace);
        List<Violation> violations = new ArrayList<>();
        int checked = 0;
        int skippedInFlight = 0;

        for (OrderEntity order : orders.findAll()) {
            if (order.getCreatedAt() != null && order.getCreatedAt().isAfter(cutoff)) {
                // Too fresh to judge — the saga may legitimately still be running.
                skippedInFlight++;
                continue;
            }
            checked++;
            checkOrder(order, seatsByOrder.get(order.getId()),
                    paymentsByOrder.getOrDefault(order.getId(), List.of()), violations);
        }

        CheckReport report = new CheckReport(checked, skippedInFlight, grace.toSeconds(), violations);
        if (!violations.isEmpty()) {
            log.warn("Invariant check found {} violation(s) across {} settled orders",
                    violations.size(), checked);
        }
        return report;
    }

    private void checkOrder(OrderEntity order, SeatView seat,
                            List<PaymentView> payments, List<Violation> out) {
        String id = order.getId();

        switch (order.getStatus()) {
            case CONFIRMED -> {
                // A confirmed order means we told the customer they own the seat.
                // Inventory must agree, or we have sold something we don't hold.
                if (seat == null || !"SOLD".equals(seat.status())) {
                    out.add(new Violation(Violation.Rule.CONFIRMED_ORDER_WITHOUT_SOLD_SEAT, id,
                            "order CONFIRMED for seat %s but inventory says %s"
                                    .formatted(order.getSeatId(),
                                            seat == null ? "no seat reserved by this order" : seat.status())));
                }
                // ...and we must actually have taken the money.
                if (payments.stream().noneMatch(p -> "AUTHORIZED".equals(p.status()))) {
                    out.add(new Violation(Violation.Rule.CONFIRMED_ORDER_WITHOUT_AUTHORIZED_PAYMENT, id,
                            "order CONFIRMED but payment-service has %s".formatted(describe(payments))));
                }
            }
            case CANCELLED -> {
                // Compensation must have released the seat. If inventory still has
                // it tied to this order, the seat is leaked — unsellable forever.
                if (seat != null && !"AVAILABLE".equals(seat.status())) {
                    out.add(new Violation(Violation.Rule.CANCELLED_ORDER_STILL_HOLDING_SEAT, id,
                            "order CANCELLED but seat %s is still %s for this order"
                                    .formatted(seat.seatId(), seat.status())));
                }
                // The one that costs real money: charged, then cancelled, never refunded.
                boolean charged = payments.stream().anyMatch(p -> "AUTHORIZED".equals(p.status()));
                boolean refunded = payments.stream().anyMatch(p -> "REFUNDED".equals(p.status()));
                if (charged && !refunded) {
                    out.add(new Violation(Violation.Rule.CANCELLED_ORDER_WITH_UNREFUNDED_PAYMENT, id,
                            "order CANCELLED but payment is still AUTHORIZED with no refund"));
                }
            }
            case PENDING -> out.add(new Violation(Violation.Rule.STUCK_PENDING_ORDER, id,
                    "order still PENDING well past the grace period — saga stalled "
                            + "(check DLQs and consumer lag)"));
        }
    }

    private String describe(List<PaymentView> payments) {
        if (payments.isEmpty()) {
            return "no payment at all";
        }
        return payments.stream().map(PaymentView::status).toList().toString();
    }

    /** Result of one scan. */
    public record CheckReport(int ordersChecked,
                              int ordersSkippedInFlight,
                              long graceSeconds,
                              List<Violation> violations) {

        public boolean consistent() {
            return violations.isEmpty();
        }
    }

    /** Minimal projection of a seat, as returned by inventory-service. */
    public record SeatView(String seatId, String status, String reservedByOrder) {
    }

    /** Minimal projection of a payment, as returned by payment-service. */
    public record PaymentView(String orderId, String status) {
    }

    /**
     * Reads the other services' state. Split behind an interface so the checker's
     * logic can be unit-tested without HTTP — the rules are the interesting part.
     */
    public interface ServiceStateReader {
        /** Seats keyed by the order currently holding them. */
        Map<String, SeatView> seatsByReservingOrder();

        /** All payments, grouped by order id (an order can have several rows). */
        Map<String, List<PaymentView>> paymentsByOrder();
    }

    /** Convenience for callers that want a single order's seat, if any. */
    public Optional<SeatView> seatFor(String orderId) {
        return Optional.ofNullable(stateReader.seatsByReservingOrder().get(orderId));
    }
}
