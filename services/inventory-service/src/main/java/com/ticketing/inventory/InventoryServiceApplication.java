package com.ticketing.inventory;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * Entry point for inventory-service.
 *
 * <p>Owns seat inventory: reserves a seat on OrderPlaced (guarded by a Redis
 * distributed lock so two buyers can't grab the same seat), releases it on
 * PaymentDeclined / OrderCancelled (compensation), and marks it sold on
 * OrderConfirmed.
 *
 * <p>{@code @EnableScheduling} drives the outbox relay.
 */
@SpringBootApplication
@EnableScheduling
public class InventoryServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(InventoryServiceApplication.class, args);
    }
}
