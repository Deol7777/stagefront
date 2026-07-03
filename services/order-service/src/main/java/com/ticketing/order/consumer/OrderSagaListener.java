package com.ticketing.order.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.ticketing.contracts.EventEnvelope;
import com.ticketing.contracts.Topics;
import com.ticketing.contracts.events.EventPayload;
import com.ticketing.contracts.events.PaymentAuthorized;
import com.ticketing.contracts.events.PaymentDeclined;
import com.ticketing.contracts.events.SeatReservationFailed;
import com.ticketing.order.app.OrderSagaService;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Kafka consumers that drive the order saga from downstream outcomes:
 * <ul>
 *   <li>{@code payments.authorized} → confirm the order</li>
 *   <li>{@code payments.declined} → cancel the order</li>
 *   <li>{@code inventory.reservation-failed} → cancel the order</li>
 * </ul>
 * Each parses the typed envelope and delegates to {@link OrderSagaService}
 * (idempotent + transactional). Failures retry then dead-letter.
 */
@Component
public class OrderSagaListener {

    private final ObjectMapper mapper;
    private final OrderSagaService saga;

    public OrderSagaListener(ObjectMapper mapper, OrderSagaService saga) {
        this.mapper = mapper;
        this.saga = saga;
    }

    @KafkaListener(topics = Topics.PAYMENTS_AUTHORIZED, groupId = "${spring.kafka.consumer.group-id}")
    public void onPaymentAuthorized(String value) throws Exception {
        saga.onPaymentAuthorized(parse(value, PaymentAuthorized.class));
    }

    @KafkaListener(topics = Topics.PAYMENTS_DECLINED, groupId = "${spring.kafka.consumer.group-id}")
    public void onPaymentDeclined(String value) throws Exception {
        saga.onPaymentDeclined(parse(value, PaymentDeclined.class));
    }

    @KafkaListener(topics = Topics.INVENTORY_RESERVATION_FAILED, groupId = "${spring.kafka.consumer.group-id}")
    public void onSeatReservationFailed(String value) throws Exception {
        saga.onSeatReservationFailed(parse(value, SeatReservationFailed.class));
    }

    /** Parse the JSON value into EventEnvelope&lt;T&gt; for the given payload type. */
    private <T extends EventPayload> EventEnvelope<T> parse(String value, Class<T> payloadType) throws Exception {
        return mapper.readValue(
                value,
                mapper.getTypeFactory().constructParametricType(EventEnvelope.class, payloadType));
    }
}
