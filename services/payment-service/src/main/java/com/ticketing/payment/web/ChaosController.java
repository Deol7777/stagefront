package com.ticketing.payment.web;

import com.ticketing.payment.gateway.GatewayControl;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

/**
 * Chaos toggles for the payment gateway — the first fault-injection surface, and the
 * seed of the future Chaos Control Panel. Flipping {@code fail} on makes every gateway
 * charge throw, which (after retries) trips the circuit breaker; flipping it off lets the
 * breaker recover through HALF_OPEN → CLOSED. Exposes the breaker's live state so the
 * dashboard (or curl) can watch the transition.
 *
 * <p>Dev/demo tool only — it deliberately lets a client degrade the service. It would not
 * exist (or would be locked down) in production.
 */
@RestController
@RequestMapping("/api/chaos")
public class ChaosController {

    private final GatewayControl control;
    private final CircuitBreakerRegistry registry;

    public ChaosController(GatewayControl control, CircuitBreakerRegistry registry) {
        this.control = control;
        this.registry = registry;
    }

    /** GET /api/chaos/gateway — current toggle + breaker state (for the dashboard tile). */
    @GetMapping("/gateway")
    public Map<String, Object> state() {
        return Map.of(
                "fail", control.isFail(),
                "latencyMs", control.getLatencyMs(),
                "breakerState", breakerState());
    }

    /**
     * POST /api/chaos/gateway?fail=true — turn the simulated gateway outage on/off.
     * Returns the same shape as GET so the caller sees the result immediately.
     */
    @PostMapping("/gateway")
    public Map<String, Object> setFail(@RequestParam boolean fail) {
        control.setFail(fail);
        return state();
    }

    /** The paymentGateway breaker's current state, or "UNKNOWN" if it hasn't been created yet. */
    private String breakerState() {
        CircuitBreaker cb = registry.find("paymentGateway").orElse(null);
        return cb == null ? "UNKNOWN" : cb.getState().name();
    }
}
