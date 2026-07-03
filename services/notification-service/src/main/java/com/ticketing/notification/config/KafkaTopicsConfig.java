package com.ticketing.notification.config;

import com.ticketing.contracts.Topics;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/**
 * DLQs for the topics this service consumes. It produces no business topics
 * (terminal service), only dead-letters for poison messages it can't handle.
 */
@Configuration
public class KafkaTopicsConfig {

    @Bean
    public NewTopic ordersConfirmedDlq() {
        return TopicBuilder.name(Topics.dlq(Topics.ORDERS_CONFIRMED)).partitions(3).replicas(1).build();
    }

    @Bean
    public NewTopic ordersCancelledDlq() {
        return TopicBuilder.name(Topics.dlq(Topics.ORDERS_CANCELLED)).partitions(3).replicas(1).build();
    }

    @Bean
    public NewTopic paymentsRefundedDlq() {
        return TopicBuilder.name(Topics.dlq(Topics.PAYMENTS_REFUNDED)).partitions(3).replicas(1).build();
    }
}
