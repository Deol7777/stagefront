package com.ticketing.payment.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.ticketing.contracts.EventEnvelope;
import com.ticketing.contracts.Topics;
import com.ticketing.contracts.events.OrderCancelled;
import com.ticketing.payment.app.RefundService;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Consumes {@code orders.cancelled} to drive refund compensation
 * (see {@link RefundService}). Failures retry then dead-letter.
 */
@Component
public class OrderCancelledListener {

    private final ObjectMapper mapper;
    private final RefundService refundService;

    public OrderCancelledListener(ObjectMapper mapper, RefundService refundService) {
        this.mapper = mapper;
        this.refundService = refundService;
    }

    @KafkaListener(topics = Topics.ORDERS_CANCELLED, groupId = "${spring.kafka.consumer.group-id}")
    public void onOrderCancelled(String value) throws Exception {
        EventEnvelope<OrderCancelled> envelope = mapper.readValue(
                value,
                mapper.getTypeFactory().constructParametricType(EventEnvelope.class, OrderCancelled.class));
        refundService.onOrderCancelled(envelope);
    }
}
