package com.ticketing.payment.gateway;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Runtime knobs for the simulated payment gateway — the "chaos" switches that let
 * us make the gateway misbehave on demand without redeploying.
 *
 * <p>Held as a singleton bean so a REST toggle (see {@code ChaosController}) and the
 * gateway client share the same mutable state. This is the seed of the future Chaos
 * Control Panel: today it's one flag; later it grows more levers.
 *
 * <p>Values are {@link AtomicBoolean}/{@link AtomicInteger} because they're read by
 * Kafka consumer threads and written by an HTTP thread concurrently — atomics give a
 * safe, lock-free visibility guarantee across those threads.
 */
@Component
public class GatewayControl {

    /** When true, every {@code charge()} fails — simulates the gateway being down. */
    private final AtomicBoolean fail = new AtomicBoolean(false);

    /** Simulated network latency per charge call, in milliseconds. */
    private final AtomicInteger latencyMs;

    public GatewayControl(@Value("${payment.gateway.latency-ms:50}") int latencyMs) {
        this.latencyMs = new AtomicInteger(latencyMs);
    }

    public boolean isFail() {
        return fail.get();
    }

    public void setFail(boolean value) {
        fail.set(value);
    }

    public int getLatencyMs() {
        return latencyMs.get();
    }

    public void setLatencyMs(int value) {
        latencyMs.set(value);
    }
}
