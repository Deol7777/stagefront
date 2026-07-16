package com.ticketing.payment.gateway;

import com.ticketing.contracts.Money;
import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import io.github.resilience4j.retry.annotation.Retry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.UUID;

/**
 * A stand-in for an external payment gateway (Stripe/Adyen/etc.) — the kind of
 * remote, over-the-network dependency a real payment service calls to actually
 * move money. We simulate it so the resilience patterns have something realistic
 * to protect: a call that can be slow or fail, unlike the old in-memory rule.
 *
 * <h2>Why the annotations, in order</h2>
 * The call is wrapped by two Resilience4j aspects. Their default order is
 * <b>Retry(outer) → CircuitBreaker(inner) → method</b>, i.e. Retry re-invokes the
 * breaker-protected call:
 *
 * <ul>
 *   <li>{@link CircuitBreaker} — tracks the failure rate over a sliding window. While
 *       the gateway is healthy the breaker is CLOSED and calls pass through. Once too
 *       many recent calls fail it trips <b>OPEN</b> and immediately rejects new calls
 *       with {@code CallNotPermittedException} — <i>without touching the gateway</i>.
 *       That's the point: stop hammering a dead dependency and give it room to recover.
 *       After a wait it goes <b>HALF_OPEN</b>, lets a few probe calls through, and
 *       CLOSES again if they succeed (or re-OPENS if they don't).</li>
 *   <li>{@link Retry} — handles the <i>transient</i> case: a single failed call is
 *       retried a few times with backoff, which papers over a momentary blip before it
 *       ever becomes a business decline. (Config makes Retry ignore
 *       {@code CallNotPermittedException}, so when the breaker is already OPEN we
 *       fast-fail instead of pointlessly retrying a rejection.)</li>
 * </ul>
 *
 * <h2>No {@code fallbackMethod} here — on purpose</h2>
 * A fallback on the inner breaker would swallow the failure before Retry could retry,
 * and a fallback that returns normally would hide the failure from the caller. Instead
 * we let the exception propagate and let {@code PaymentService} decide the business
 * outcome (a graceful {@code GATEWAY_UNAVAILABLE} decline). Keeping the policy (retry +
 * break) here and the decision (decline + compensate) in the service is a clean split.
 */
@Component
public class PaymentGatewayClient {

    private static final Logger log = LoggerFactory.getLogger(PaymentGatewayClient.class);

    /** Must match the instance names in application.yml (resilience4j.*.instances.paymentGateway). */
    private static final String GATEWAY = "paymentGateway";

    private final GatewayControl control;

    public PaymentGatewayClient(GatewayControl control) {
        this.control = control;
    }

    /**
     * "Charge" the card and return a gateway authorization code.
     *
     * @throws GatewayException if the simulated gateway is failing (counted by the
     *                          breaker, retried by Retry)
     * @throws io.github.resilience4j.circuitbreaker.CallNotPermittedException if the
     *                          breaker is OPEN (thrown by the CircuitBreaker aspect,
     *                          not by this body)
     */
    @CircuitBreaker(name = GATEWAY)
    @Retry(name = GATEWAY)
    public String charge(String orderId, Money money) {
        // Simulate the network round-trip. A real breaker would also be paired with a
        // timeout (Resilience4j @TimeLimiter) so a *slow* gateway can't hang the caller;
        // we skip it here because @TimeLimiter forces an async CompletableFuture return
        // and the extra plumbing isn't worth it for the demo. The sleep stands in for
        // latency only.
        sleep(control.getLatencyMs());

        if (control.isFail()) {
            // The simulated failure. Each throw is one "failure" in the breaker's window
            // and one attempt consumed by Retry.
            log.warn("Payment gateway call FAILED for order {} (chaos: gateway.fail=true)", orderId);
            throw new GatewayException("payment gateway unavailable");
        }

        String authCode = "auth-" + UUID.randomUUID().toString().substring(0, 8);
        log.info("Payment gateway authorized order {} → {} ({} {})",
                orderId, authCode, money.amount(), money.currency());
        return authCode;
    }

    private void sleep(int ms) {
        if (ms <= 0) {
            return;
        }
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
