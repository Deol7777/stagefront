package com.ticketing.payment.config;

import com.ticketing.contracts.Topics;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/** Topics this service produces, plus the DLQ for its SeatReserved consumer. */
@Configuration
public class KafkaTopicsConfig {

    @Bean
    public NewTopic paymentsAuthorizedTopic() {
        return TopicBuilder.name(Topics.PAYMENTS_AUTHORIZED).partitions(3).replicas(1).build();
    }

    @Bean
    public NewTopic paymentsDeclinedTopic() {
        return TopicBuilder.name(Topics.PAYMENTS_DECLINED).partitions(3).replicas(1).build();
    }

    @Bean
    public NewTopic inventoryReservedDlqTopic() {
        return TopicBuilder.name(Topics.dlq(Topics.INVENTORY_RESERVED)).partitions(3).replicas(1).build();
    }
}
