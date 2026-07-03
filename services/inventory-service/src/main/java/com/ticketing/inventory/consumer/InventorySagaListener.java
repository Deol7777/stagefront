package com.ticketing.inventory.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.ticketing.contracts.EventEnvelope;
import com.ticketing.contracts.Topics;
import com.ticketing.contracts.events.EventPayload;
import com.ticketing.contracts.events.OrderCancelled;
import com.ticketing.contracts.events.OrderConfirmed;
import com.ticketing.contracts.events.PaymentDeclined;
import com.ticketing.inventory.app.InventorySagaService;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Consumers for the compensation / finalization events:
 * <ul>
 *   <li>{@code payments.declined} → release the seat (compensation)</li>
 *   <li>{@code orders.cancelled} → release the seat (compensation)</li>
 *   <li>{@code orders.confirmed} → mark the seat sold (finalization)</li>
 * </ul>
 * Delegates to {@link InventorySagaService}; failures retry then dead-letter.
 */
@Component
public class InventorySagaListener {

    private final ObjectMapper mapper;
    private final InventorySagaService saga;

    public InventorySagaListener(ObjectMapper mapper, InventorySagaService saga) {
        this.mapper = mapper;
        this.saga = saga;
    }

    @KafkaListener(topics = Topics.PAYMENTS_DECLINED, groupId = "${spring.kafka.consumer.group-id}")
    public void onPaymentDeclined(String value) throws Exception {
        saga.onPaymentDeclined(parse(value, PaymentDeclined.class));
    }

    @KafkaListener(topics = Topics.ORDERS_CANCELLED, groupId = "${spring.kafka.consumer.group-id}")
    public void onOrderCancelled(String value) throws Exception {
        saga.onOrderCancelled(parse(value, OrderCancelled.class));
    }

    @KafkaListener(topics = Topics.ORDERS_CONFIRMED, groupId = "${spring.kafka.consumer.group-id}")
    public void onOrderConfirmed(String value) throws Exception {
        saga.onOrderConfirmed(parse(value, OrderConfirmed.class));
    }

    private <T extends EventPayload> EventEnvelope<T> parse(String value, Class<T> payloadType) throws Exception {
        return mapper.readValue(
                value,
                mapper.getTypeFactory().constructParametricType(EventEnvelope.class, payloadType));
    }
}
