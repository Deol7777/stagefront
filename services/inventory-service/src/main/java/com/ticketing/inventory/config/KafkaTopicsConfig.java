package com.ticketing.inventory.config;

import com.ticketing.contracts.Topics;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/**
 * Topics this service produces, plus the DLQ it writes poison messages to.
 * KafkaAdmin creates these on startup (broker auto-create is off).
 */
@Configuration
public class KafkaTopicsConfig {

    @Bean
    public NewTopic seatReservedTopic() {
        return TopicBuilder.name(Topics.INVENTORY_RESERVED).partitions(3).replicas(1).build();
    }

    @Bean
    public NewTopic seatReservationFailedTopic() {
        return TopicBuilder.name(Topics.INVENTORY_RESERVATION_FAILED).partitions(3).replicas(1).build();
    }

    /**
     * Dead-letter topic for the orders.placed consumer. Same partition count as
     * the source so the recoverer can keep a record on its original partition.
     */
    @Bean
    public NewTopic ordersPlacedDlqTopic() {
        return TopicBuilder.name(Topics.dlq(Topics.ORDERS_PLACED)).partitions(3).replicas(1).build();
    }
}
