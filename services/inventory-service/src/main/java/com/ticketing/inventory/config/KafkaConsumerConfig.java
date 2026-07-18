package com.ticketing.inventory.config;

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
 * Wires retry + dead-letter behaviour for Kafka consumers.
 *
 * <p>When a listener throws, the {@link DefaultErrorHandler} retries with a fixed
 * backoff. Once retries are exhausted, the {@link DeadLetterPublishingRecoverer}
 * republishes the failed record to a dead-letter topic so it is NOT silently
 * dropped and the partition is not blocked. We can inspect and replay the DLQ
 * later (replay is safe because consumers are idempotent).
 */
@Configuration
public class KafkaConsumerConfig {

    /**
     * Error handler = retry then dead-letter.
     * FixedBackOff(500ms, 2) → 1 initial try + 2 retries = 3 attempts total.
     * Destination = original topic name + ".DLQ", same partition (our convention).
     */
    @Bean
    public DefaultErrorHandler errorHandler(KafkaTemplate<String, String> template) {
        var recoverer = new DeadLetterPublishingRecoverer(
                template,
                (record, exception) -> new TopicPartition(record.topic() + ".DLQ", record.partition()));
        return new DefaultErrorHandler(recoverer, new FixedBackOff(500L, 2L));
    }

    /**
     * The listener container factory @KafkaListener uses. We attach the error
     * handler so every listener gets the retry + DLQ behaviour. The
     * ConsumerFactory is auto-configured by Spring Boot from application.yml.
     */
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory(
            ConsumerFactory<String, String> consumerFactory, DefaultErrorHandler errorHandler) {
        var factory = new ConcurrentKafkaListenerContainerFactory<String, String>();
        factory.setConsumerFactory(consumerFactory);
        factory.setCommonErrorHandler(errorHandler);
        // Turn on consumer-side observation EXPLICITLY.
        //
        // `spring.kafka.listener.observation-enabled: true` in application.yml only
        // configures the factory Spring Boot builds for you. The moment you declare
        // your own factory bean (we do, to attach the DLQ error handler) that
        // auto-configured one is replaced and the property silently does nothing.
        // Silently is the dangerous part: producers still stamp `traceparent` on
        // records, so traces look fine until you notice consumers never join them
        // and every saga is split at the topic boundary.
        //
        // With this on, the container extracts `traceparent` off the record and
        // runs the listener as a child of the producing span.
        factory.getContainerProperties().setObservationEnabled(true);
        return factory;
    }
}
