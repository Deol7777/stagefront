package com.ticketing.payment.gateway;

/**
 * Thrown by {@link PaymentGatewayClient#charge} when the simulated gateway call
 * fails (the "network" call errored). This is the exception the circuit breaker
 * counts as a failure and the retry re-attempts. It stands in for the connection
 * timeouts / 5xx responses a real payment gateway would throw.
 */
public class GatewayException extends RuntimeException {
    public GatewayException(String message) {
        super(message);
    }
}
