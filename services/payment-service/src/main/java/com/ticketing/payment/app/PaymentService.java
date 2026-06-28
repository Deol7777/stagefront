package com.ticketing.payment.app;

import com.ticketing.contracts.EventEnvelope;
import com.ticketing.contracts.Money;
import com.ticketing.contracts.events.EventType;
import com.ticketing.contracts.events.PaymentAuthorized;
import com.ticketing.contracts.events.PaymentDeclineReason;
import com.ticketing.contracts.events.PaymentDeclined;
import com.ticketing.contracts.events.SeatReserved;
import com.ticketing.payment.consumer.ProcessedEvent;
import com.ticketing.payment.consumer.ProcessedEventRepository;
import com.ticketing.payment.domain.PaymentEntity;
import com.ticketing.payment.domain.PaymentRepository;
import com.ticketing.payment.domain.PaymentStatus;
import com.ticketing.payment.outbox.OutboxWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

/**
 * Authorizes or declines payment for a reserved seat. Idempotent + transactional:
 * the payment row, the dedup record, and the outbox event all commit together.
 *
 * <p>The decision is a stand-in for a real payment gateway: decline any amount
 * over a configured threshold. That single deterministic rule lets us exercise
 * the decline → compensation path on demand (just order something expensive).
 */
@Service
public class PaymentService {

    private static final Logger log = LoggerFactory.getLogger(PaymentService.class);
    private static final String CONSUMER = "payment-service";

    private final PaymentRepository payments;
    private final ProcessedEventRepository processed;
    private final OutboxWriter outbox;
    private final BigDecimal declineAbove;

    public PaymentService(PaymentRepository payments, ProcessedEventRepository processed,
                          OutboxWriter outbox, @Value("${payment.decline-above:1000.00}") BigDecimal declineAbove) {
        this.payments = payments;
        this.processed = processed;
        this.outbox = outbox;
        this.declineAbove = declineAbove;
    }

    @Transactional
    public void handle(EventEnvelope<SeatReserved> envelope) {
        SeatReserved seat = envelope.payload();
        String dedupKey = seat.dedupKey();   // = reservationId

        if (processed.existsById(dedupKey)) {
            log.info("Duplicate SeatReserved for reservation {} — skipping (idempotent)", seat.reservationId());
            return;
        }

        Money money = seat.amount();
        String paymentId = UUID.randomUUID().toString();
        boolean authorize = money.amount().compareTo(declineAbove) <= 0;
        String traceId = envelope.traceId();

        if (authorize) {
            payments.save(new PaymentEntity(paymentId, seat.orderId(), seat.reservationId(),
                    money.amount(), money.currency(), PaymentStatus.AUTHORIZED, Instant.now()));
            var event = new PaymentAuthorized(seat.orderId(), paymentId, money, Instant.now());
            outbox.append(EventType.PAYMENT_AUTHORIZED, "Payment", paymentId, event, traceId);
            log.info("Authorized payment {} for order {} ({} {})",
                    paymentId, seat.orderId(), money.amount(), money.currency());
        } else {
            payments.save(new PaymentEntity(paymentId, seat.orderId(), seat.reservationId(),
                    money.amount(), money.currency(), PaymentStatus.DECLINED, Instant.now()));
            var event = new PaymentDeclined(seat.orderId(), paymentId, PaymentDeclineReason.INSUFFICIENT_FUNDS);
            outbox.append(EventType.PAYMENT_DECLINED, "Payment", paymentId, event, traceId);
            log.info("Declined payment {} for order {} (amount {} > {})",
                    paymentId, seat.orderId(), money.amount(), declineAbove);
        }

        processed.save(new ProcessedEvent(dedupKey, CONSUMER, Instant.now()));
    }
}
