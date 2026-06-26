package com.ticketing.inventory;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Entry point for inventory-service.
 *
 * <p>Owns seat inventory: reserves a seat on OrderPlaced (guarded by a Redis
 * distributed lock so two buyers can't grab the same seat), releases it on
 * PaymentDeclined / OrderCancelled (compensation), and marks it sold on
 * OrderConfirmed. Uses Redis cache-aside for hot seat-availability reads.
 *
 * <p>Skeleton only: empty context with web + actuator for now.
 */
@SpringBootApplication
public class InventoryServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(InventoryServiceApplication.class, args);
    }
}
