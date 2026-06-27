package com.ticketing.order;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * Entry point for order-service.
 *
 * <p>Owns the order lifecycle (PENDING -> CONFIRMED / CANCELLED). Accepts ticket
 * orders over REST, then drives the saga by producing OrderPlaced and reacting
 * to PaymentAuthorized / PaymentDeclined / SeatReservationFailed events.
 *
 * <p>{@code @EnableScheduling} turns on the timer that drives the outbox relay.
 */
@SpringBootApplication // = @Configuration + @EnableAutoConfiguration + @ComponentScan
@EnableScheduling
public class OrderServiceApplication {

    public static void main(String[] args) {
        // Boots the Spring context and starts the embedded web server.
        SpringApplication.run(OrderServiceApplication.class, args);
    }
}
