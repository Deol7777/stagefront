package com.ticketing.notification;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Entry point for notification-service.
 *
 * <p>Consumes terminal saga events (OrderConfirmed, OrderCancelled,
 * PaymentRefunded) and sends the matching user notification. Must be an
 * idempotent consumer — duplicate delivery must not send two notifications.
 *
 * <p>Skeleton only: empty context with web + actuator for now.
 */
@SpringBootApplication
public class NotificationServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(NotificationServiceApplication.class, args);
    }
}
