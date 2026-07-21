package com.ticketing.order.dlq;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The replay decision — re-emit vs. park — that stops a poison message from
 * compounding the DLQ. The Kafka plumbing (offset commits, header round-trip) is
 * exercised live; this pins the pure policy that drives it.
 */
class DlqReplayPolicyTest {

    @Test
    void re_emits_while_attempts_remain() {
        for (int attempts = 0; attempts < DlqService.MAX_REPLAY_ATTEMPTS; attempts++) {
            assertFalse(DlqService.shouldPark(attempts),
                    "attempt " + attempts + " should still be re-emitted, not parked");
        }
    }

    @Test
    void parks_once_attempts_are_exhausted() {
        assertTrue(DlqService.shouldPark(DlqService.MAX_REPLAY_ATTEMPTS));
        assertTrue(DlqService.shouldPark(DlqService.MAX_REPLAY_ATTEMPTS + 5),
                "a record already past the cap must stay parked, never loop back to source");
    }

    @Test
    void missing_or_garbage_attempts_header_counts_as_zero() {
        // A record that never carried the header (first time through, or produced by
        // the recoverer without it) must be treated as a fresh attempt, not skipped.
        assertEquals(0, DlqService.parseAttempts(null));
        assertEquals(0, DlqService.parseAttempts("not-a-number"));
        assertEquals(0, DlqService.parseAttempts("-4"), "negative is clamped to 0");
    }

    @Test
    void parses_a_real_attempts_value() {
        assertEquals(2, DlqService.parseAttempts("2"));
        assertEquals(2, DlqService.parseAttempts(" 2 "), "whitespace tolerated");
    }
}
