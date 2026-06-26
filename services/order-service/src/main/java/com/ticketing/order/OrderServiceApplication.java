package com.ticketing.order;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Entry point for order-service.
 *
 * <p>Owns the order lifecycle (PENDING -> CONFIRMED / CANCELLED). Accepts ticket
 * orders over REST, then drives the saga by producing OrderPlaced and reacting
 * to PaymentAuthorized / PaymentDeclined / SeatReservationFailed events.
 *
 * <p>Skeleton only right now: boots an empty Spring context with web + actuator.
 * Persistence, outbox, Kafka, and saga handlers are added in later steps.
 */
@SpringBootApplication // = @Configuration + @EnableAutoConfiguration + @ComponentScan
public class OrderServiceApplication {

    public static void main(String[] args) {
        // Boots the Spring context and starts the embedded web server.
        SpringApplication.run(OrderServiceApplication.class, args);
    }
}
