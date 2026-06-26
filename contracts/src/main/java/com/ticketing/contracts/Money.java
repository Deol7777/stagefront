package com.ticketing.contracts;

import java.math.BigDecimal;

/**
 * A money amount + its currency. A small value object so we never pass a bare
 * {@code BigDecimal} around and lose track of the currency.
 *
 * <p>{@link BigDecimal} (not double) because money must be exact — floating
 * point can't represent 0.10 precisely, and rounding errors in money are bugs.
 *
 * @param amount   the value, e.g. 49.99 (never null)
 * @param currency ISO-4217 code, e.g. "USD" (never null)
 */
public record Money(BigDecimal amount, String currency) {
}
