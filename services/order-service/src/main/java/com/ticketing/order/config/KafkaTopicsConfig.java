package com.ticketing.order.config;

import com.ticketing.contracts.Topics;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/**
 * Declares the Kafka topics this service produces. Spring's KafkaAdmin creates
 * any {@link NewTopic} bean on startup — we do this explicitly because the broker
 * has auto-create disabled (so topic names + partition counts are deliberate).
 *
 * <p>3 partitions = up to 3 consumers in a group processing in parallel. Events
 * keyed by orderId still stay ordered (same key → same partition). replicas=1
 * because the local broker is single-node.
 */
@Configuration
public class KafkaTopicsConfig {

    @Bean
    public NewTopic ordersPlacedTopic() {
        return TopicBuilder.name(Topics.ORDERS_PLACED)
                .partitions(3)
                .replicas(1)
                .build();
    }

    // Terminal saga events this service produces.
    @Bean
    public NewTopic ordersConfirmedTopic() {
        return TopicBuilder.name(Topics.ORDERS_CONFIRMED).partitions(3).replicas(1).build();
    }

    @Bean
    public NewTopic ordersCancelledTopic() {
        return TopicBuilder.name(Topics.ORDERS_CANCELLED).partitions(3).replicas(1).build();
    }

    // DLQs for the topics this service consumes (saga outcomes).
    @Bean
    public NewTopic paymentsAuthorizedDlq() {
        return TopicBuilder.name(Topics.dlq(Topics.PAYMENTS_AUTHORIZED)).partitions(3).replicas(1).build();
    }

    @Bean
    public NewTopic paymentsDeclinedDlq() {
        return TopicBuilder.name(Topics.dlq(Topics.PAYMENTS_DECLINED)).partitions(3).replicas(1).build();
    }

    @Bean
    public NewTopic reservationFailedDlq() {
        return TopicBuilder.name(Topics.dlq(Topics.INVENTORY_RESERVATION_FAILED)).partitions(3).replicas(1).build();
    }
}
