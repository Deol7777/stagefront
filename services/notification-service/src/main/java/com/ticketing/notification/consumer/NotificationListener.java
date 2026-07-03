package com.ticketing.notification.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.ticketing.contracts.EventEnvelope;
import com.ticketing.contracts.Topics;
import com.ticketing.contracts.events.EventPayload;
import com.ticketing.contracts.events.OrderCancelled;
import com.ticketing.contracts.events.OrderConfirmed;
import com.ticketing.contracts.events.PaymentRefunded;
import com.ticketing.notification.app.NotificationService;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Consumes the terminal saga events and turns each into a notification:
 * orders.confirmed, orders.cancelled, payments.refunded.
 * Failures retry then dead-letter.
 */
@Component
public class NotificationListener {

    private final ObjectMapper mapper;
    private final NotificationService service;

    public NotificationListener(ObjectMapper mapper, NotificationService service) {
        this.mapper = mapper;
        this.service = service;
    }

    @KafkaListener(topics = Topics.ORDERS_CONFIRMED, groupId = "${spring.kafka.consumer.group-id}")
    public void onOrderConfirmed(String value) throws Exception {
        service.onOrderConfirmed(parse(value, OrderConfirmed.class));
    }

    @KafkaListener(topics = Topics.ORDERS_CANCELLED, groupId = "${spring.kafka.consumer.group-id}")
    public void onOrderCancelled(String value) throws Exception {
        service.onOrderCancelled(parse(value, OrderCancelled.class));
    }

    @KafkaListener(topics = Topics.PAYMENTS_REFUNDED, groupId = "${spring.kafka.consumer.group-id}")
    public void onPaymentRefunded(String value) throws Exception {
        service.onPaymentRefunded(parse(value, PaymentRefunded.class));
    }

    private <T extends EventPayload> EventEnvelope<T> parse(String value, Class<T> payloadType) throws Exception {
        return mapper.readValue(
                value,
                mapper.getTypeFactory().constructParametricType(EventEnvelope.class, payloadType));
    }
}
