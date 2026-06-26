package com.ticketing.payment;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Entry point for payment-service.
 *
 * <p>Authorizes payment on SeatReserved, declines on failure (triggering
 * compensation upstream), and issues refunds (PaymentRefunded) when a charged
 * order is cancelled. A fault target for the chaos demos (force a decline
 * mid-saga to prove compensation works).
 *
 * <p>Skeleton only: empty context with web + actuator for now.
 */
@SpringBootApplication
public class PaymentServiceApplication {

    public static void main(String[] args) {
        SpringApplication.run(PaymentServiceApplication.class, args);
    }
}
