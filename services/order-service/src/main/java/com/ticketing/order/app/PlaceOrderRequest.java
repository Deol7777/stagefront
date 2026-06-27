package com.ticketing.order.app;

import java.math.BigDecimal;

/**
 * Input to place an order (the POST body).
 *
 * @param userId          buyer
 * @param seatId          seat being bought
 * @param eventScheduleId which show/date
 * @param amount          price
 * @param currency        ISO-4217 code
 * @param requestId       client idempotency token for the request (lets us dedup
 *                        a retried POST later; carried into the OrderPlaced event)
 */
public record PlaceOrderRequest(
        String userId,
        String seatId,
        String eventScheduleId,
        BigDecimal amount,
        String currency,
        String requestId) {
}
