package com.ticketing.notification.app;

import com.ticketing.contracts.EventEnvelope;
import com.ticketing.contracts.events.OrderCancelled;
import com.ticketing.contracts.events.OrderConfirmed;
import com.ticketing.contracts.events.PaymentRefunded;
import com.ticketing.notification.consumer.ProcessedEvent;
import com.ticketing.notification.consumer.ProcessedEventRepository;
import com.ticketing.notification.domain.NotificationEntity;
import com.ticketing.notification.domain.NotificationRepository;
import com.ticketing.notification.domain.NotificationType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.UUID;

/**
 * Turns terminal saga events into user notifications. "Sending" = persist a row
 * and log it (no real email/SMS in the demo). Idempotent: a duplicate event must
 * not notify the user twice — the dedup gate prevents it, and the save +
 * dedup-record commit in one transaction.
 */
@Service
public class NotificationService {

    private static final Logger log = LoggerFactory.getLogger(NotificationService.class);
    private static final String CONSUMER = "notification-service";

    private final NotificationRepository notifications;
    private final ProcessedEventRepository processed;

    public NotificationService(NotificationRepository notifications, ProcessedEventRepository processed) {
        this.notifications = notifications;
        this.processed = processed;
    }

    @Transactional
    public void onOrderConfirmed(EventEnvelope<OrderConfirmed> env) {
        OrderConfirmed e = env.payload();
        if (alreadyHandled(env.eventType(), e.dedupKey())) return;
        send(e.orderId(), e.userId(), NotificationType.ORDER_CONFIRMED,
                "Your order " + e.orderId() + " is confirmed — seat " + e.seatId() + " is yours.");
        record(env.eventType(), e.dedupKey());
    }

    @Transactional
    public void onOrderCancelled(EventEnvelope<OrderCancelled> env) {
        OrderCancelled e = env.payload();
        if (alreadyHandled(env.eventType(), e.dedupKey())) return;
        send(e.orderId(), e.userId(), NotificationType.ORDER_CANCELLED,
                "Your order " + e.orderId() + " was cancelled (" + e.reason() + ").");
        record(env.eventType(), e.dedupKey());
    }

    @Transactional
    public void onPaymentRefunded(EventEnvelope<PaymentRefunded> env) {
        PaymentRefunded e = env.payload();
        if (alreadyHandled(env.eventType(), e.dedupKey())) return;
        // PaymentRefunded carries no userId; key the notification by order.
        send(e.orderId(), "unknown", NotificationType.PAYMENT_REFUNDED,
                "Refund of " + e.amount().amount() + " " + e.amount().currency()
                        + " issued for order " + e.orderId() + ".");
        record(env.eventType(), e.dedupKey());
    }

    private void send(String orderId, String userId, NotificationType type, String message) {
        notifications.save(new NotificationEntity(
                UUID.randomUUID(), orderId, userId, type, message, Instant.now()));
        // The "delivery" — a log line stands in for email/SMS.
        log.info("NOTIFY [{}] order {} -> {}", type, orderId, message);
    }

    private boolean alreadyHandled(String eventType, String dedupKey) {
        String key = eventType + "#" + dedupKey;
        if (processed.existsById(key)) {
            log.info("Duplicate {} — skipping (idempotent)", key);
            return true;
        }
        return false;
    }

    private void record(String eventType, String dedupKey) {
        processed.save(new ProcessedEvent(eventType + "#" + dedupKey, CONSUMER, Instant.now()));
    }
}
