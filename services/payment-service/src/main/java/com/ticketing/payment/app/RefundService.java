package com.ticketing.payment.app;

import com.ticketing.contracts.EventEnvelope;
import com.ticketing.contracts.Money;
import com.ticketing.contracts.events.EventType;
import com.ticketing.contracts.events.OrderCancelled;
import com.ticketing.contracts.events.PaymentRefunded;
import com.ticketing.payment.consumer.ProcessedEvent;
import com.ticketing.payment.consumer.ProcessedEventRepository;
import com.ticketing.payment.domain.PaymentEntity;
import com.ticketing.payment.domain.PaymentRepository;
import com.ticketing.payment.domain.PaymentStatus;
import com.ticketing.payment.outbox.OutboxWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.UUID;

/**
 * Refund compensation. When an order is cancelled AFTER its payment was
 * authorized (e.g. a user cancels a confirmed order), the captured charge must
 * be reversed. This consumes OrderCancelled and, if an authorized payment exists
 * for that order, refunds it and emits PaymentRefunded.
 *
 * <p>Doubly safe: idempotent (dedup on the cancellation event) AND guarded by
 * payment status — once a payment is REFUNDED it's no longer AUTHORIZED, so a
 * replayed OrderCancelled finds nothing to refund and no-ops. Orders that were
 * never paid (declined / cancelled pre-payment) simply have no authorized
 * payment, so nothing happens.
 */
@Service
public class RefundService {

    private static final Logger log = LoggerFactory.getLogger(RefundService.class);
    private static final String CONSUMER = "payment-service";

    private final PaymentRepository payments;
    private final ProcessedEventRepository processed;
    private final OutboxWriter outbox;

    public RefundService(PaymentRepository payments, ProcessedEventRepository processed, OutboxWriter outbox) {
        this.payments = payments;
        this.processed = processed;
        this.outbox = outbox;
    }

    @Transactional
    public void onOrderCancelled(EventEnvelope<OrderCancelled> env) {
        OrderCancelled c = env.payload();
        String key = env.eventType() + "#" + c.dedupKey();
        if (processed.existsById(key)) {
            log.info("Duplicate {} — skipping (idempotent)", key);
            return;
        }

        // Refund only if there is an authorized payment for this order.
        payments.findFirstByOrderIdAndStatus(c.orderId(), PaymentStatus.AUTHORIZED)
                .ifPresent(payment -> refund(payment, env.traceId()));

        processed.save(new ProcessedEvent(key, CONSUMER, Instant.now()));
    }

    private void refund(PaymentEntity payment, String traceId) {
        payment.markRefunded();
        String refundId = UUID.randomUUID().toString();
        var money = new Money(payment.getAmount(), payment.getCurrency());
        var event = new PaymentRefunded(
                payment.getOrderId(), payment.getPaymentId(), refundId, money, Instant.now());
        outbox.append(EventType.PAYMENT_REFUNDED, "Payment", payment.getPaymentId(), event, traceId);
        log.info("Refunded payment {} for order {} (refund {})",
                payment.getPaymentId(), payment.getOrderId(), refundId);
    }
}
