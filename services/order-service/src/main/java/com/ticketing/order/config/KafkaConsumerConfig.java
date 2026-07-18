package com.ticketing.order.config;

import org.apache.kafka.common.TopicPartition;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

/**
 * Retry + dead-letter behaviour for the saga consumers (see note 05). Retries
 * with backoff, then routes exhausted records to {@code <topic>.DLQ}.
 */
@Configuration
public class KafkaConsumerConfig {

    @Bean
    public DefaultErrorHandler errorHandler(KafkaTemplate<String, String> template) {
        var recoverer = new DeadLetterPublishingRecoverer(
                template,
                (record, exception) -> new TopicPartition(record.topic() + ".DLQ", record.partition()));
        return new DefaultErrorHandler(recoverer, new FixedBackOff(500L, 2L));
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory(
            ConsumerFactory<String, String> consumerFactory, DefaultErrorHandler errorHandler) {
        var factory = new ConcurrentKafkaListenerContainerFactory<String, String>();
        factory.setConsumerFactory(consumerFactory);
        factory.setCommonErrorHandler(errorHandler);
        // Consumer-side observation must be set on OUR factory: the
        // `spring.kafka.listener.observation-enabled` property only reaches the
        // factory Boot auto-configures, which declaring this bean replaces.
        // Without it, consumers never join the producer's trace and every saga
        // is silently split at each topic boundary. See inventory-service for
        // the longer note.
        factory.getContainerProperties().setObservationEnabled(true);
        return factory;
    }
}
