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
import com.ticketing.payment.gateway.GatewayException;
import com.ticketing.payment.gateway.PaymentGatewayClient;
import com.ticketing.payment.outbox.OutboxWriter;
import io.github.resilience4j.circuitbreaker.CallNotPermittedException;
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
    private final PaymentGatewayClient gateway;
    private final BigDecimal declineAbove;

    public PaymentService(PaymentRepository payments, ProcessedEventRepository processed,
                          OutboxWriter outbox, PaymentGatewayClient gateway,
                          @Value("${payment.decline-above:1000.00}") BigDecimal declineAbove) {
        this.payments = payments;
        this.processed = processed;
        this.outbox = outbox;
        this.gateway = gateway;
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
        String traceId = envelope.traceId();

        // Two gates to authorization:
        //   1. business rule — is the amount within the allowed limit?
        //   2. gateway call  — does the (simulated) external gateway approve the charge?
        // Failing gate 1 → INSUFFICIENT_FUNDS. A gateway outage at gate 2 →
        // GATEWAY_UNAVAILABLE. BOTH are graceful declines that let the saga compensate;
        // neither throws, so a gateway outage does NOT dead-letter the message. That is
        // the whole reason for the circuit breaker: convert a hanging/cascading failure
        // into a fast, clean business decline.
        if (money.amount().compareTo(declineAbove) > 0) {
            decline(paymentId, seat, money, PaymentDeclineReason.INSUFFICIENT_FUNDS, traceId);
            log.info("Declined payment {} for order {} (amount {} > {})",
                    paymentId, seat.orderId(), money.amount(), declineAbove);
        } else {
            try {
                // External call guarded by @CircuitBreaker + @Retry (see PaymentGatewayClient).
                // Done BEFORE any DB write so the transaction's connection isn't held open
                // across the network call — real systems keep I/O out of the DB transaction.
                String authCode = gateway.charge(seat.orderId(), money);
                authorize(paymentId, seat, money, traceId);
                log.info("Authorized payment {} for order {} via gateway {} ({} {})",
                        paymentId, seat.orderId(), authCode, money.amount(), money.currency());
            } catch (GatewayException | CallNotPermittedException e) {
                // GatewayException          → the charge failed after Retry exhausted its attempts.
                // CallNotPermittedException → the breaker is OPEN and fast-failed the call.
                // Either way the gateway is effectively unavailable → graceful decline.
                decline(paymentId, seat, money, PaymentDeclineReason.GATEWAY_UNAVAILABLE, traceId);
                log.warn("Declined payment {} for order {} — gateway unavailable ({})",
                        paymentId, seat.orderId(), e.getClass().getSimpleName());
            }
        }

        processed.save(new ProcessedEvent(dedupKey, CONSUMER, Instant.now()));
    }

    /** Persist an AUTHORIZED payment and emit PaymentAuthorized through the outbox. */
    private void authorize(String paymentId, SeatReserved seat, Money money, String traceId) {
        payments.save(new PaymentEntity(paymentId, seat.orderId(), seat.reservationId(),
                money.amount(), money.currency(), PaymentStatus.AUTHORIZED, Instant.now()));
        var event = new PaymentAuthorized(seat.orderId(), paymentId, money, Instant.now());
        outbox.append(EventType.PAYMENT_AUTHORIZED, "Payment", paymentId, event, traceId);
    }

    /** Persist a DECLINED payment and emit PaymentDeclined (with the given reason). */
    private void decline(String paymentId, SeatReserved seat, Money money,
                         PaymentDeclineReason reason, String traceId) {
        payments.save(new PaymentEntity(paymentId, seat.orderId(), seat.reservationId(),
                money.amount(), money.currency(), PaymentStatus.DECLINED, Instant.now()));
        var event = new PaymentDeclined(seat.orderId(), paymentId, reason);
        outbox.append(EventType.PAYMENT_DECLINED, "Payment", paymentId, event, traceId);
    }
}
