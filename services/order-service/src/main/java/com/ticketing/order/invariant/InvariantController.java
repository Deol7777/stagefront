package com.ticketing.order.invariant;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.Duration;

/**
 * Read-only reconciliation endpoint: does the system as a whole still make sense?
 *
 * <p>Deliberately a pull, not a background job. A scheduled checker that alerts on
 * its own is the natural next step, but keeping it on-demand first makes the
 * grace-period trade-off visible — you can watch violations appear and then clear
 * themselves as an in-flight saga completes.
 */
@RestController
@RequestMapping("/api/invariants")
public class InvariantController {

    private final InvariantChecker checker;

    public InvariantController(InvariantChecker checker) {
        this.checker = checker;
    }

    /**
     * GET /api/invariants?graceSeconds=30
     *
     * <p>Returns 200 when consistent, 409 CONFLICT when violations were found — so
     * a script or a health probe can act on the status code alone. The body is the
     * same either way.
     *
     * @param graceSeconds how long an order must have been settled before it is
     *                     judged. Raise it if healthy in-flight sagas get flagged;
     *                     lower it to catch breakage sooner.
     */
    @GetMapping
    public ResponseEntity<InvariantChecker.CheckReport> check(
            @RequestParam(defaultValue = "30") long graceSeconds) {
        var report = checker.check(Duration.ofSeconds(graceSeconds));
        return report.consistent()
                ? ResponseEntity.ok(report)
                : ResponseEntity.status(HttpStatus.CONFLICT).body(report);
    }
}
