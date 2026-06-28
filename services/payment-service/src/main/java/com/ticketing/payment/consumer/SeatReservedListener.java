package com.ticketing.payment.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.ticketing.contracts.EventEnvelope;
import com.ticketing.contracts.Topics;
import com.ticketing.contracts.events.SeatReserved;
import com.ticketing.payment.app.PaymentService;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Kafka consumer for {@code inventory.reserved}. Parses the SeatReserved envelope
 * and hands it to {@link PaymentService}. No distributed lock needed — payment
 * has no shared contended resource (idempotency alone keeps it safe).
 *
 * <p>A parse failure or repeated error is retried then dead-lettered to
 * {@code inventory.reserved.DLQ} (see KafkaConsumerConfig).
 */
@Component
public class SeatReservedListener {

    private final ObjectMapper mapper;
    private final PaymentService paymentService;

    public SeatReservedListener(ObjectMapper mapper, PaymentService paymentService) {
        this.mapper = mapper;
        this.paymentService = paymentService;
    }

    @KafkaListener(topics = Topics.INVENTORY_RESERVED, groupId = "${spring.kafka.consumer.group-id}")
    public void onSeatReserved(String value) throws Exception {
        EventEnvelope<SeatReserved> envelope = mapper.readValue(
                value,
                mapper.getTypeFactory().constructParametricType(EventEnvelope.class, SeatReserved.class));
        paymentService.handle(envelope);
    }
}
