package com.ticketing.inventory.web;

import com.ticketing.inventory.cache.SeatCache;
import com.ticketing.inventory.cache.SeatQueryService;
import com.ticketing.inventory.cache.SeatView;
import com.ticketing.inventory.domain.SeatEntity;
import com.ticketing.inventory.domain.SeatRepository;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

/** Read-only seat endpoints for the debug dashboard, plus the cache-aside single-seat read. */
@RestController
@RequestMapping("/api/seats")
public class SeatController {

    private final SeatRepository seats;
    private final SeatQueryService query;
    private final SeatCache cache;

    public SeatController(SeatRepository seats, SeatQueryService query, SeatCache cache) {
        this.seats = seats;
        this.query = query;
        this.cache = cache;
    }

    /**
     * GET /api/seats — list ALL seats, straight from the DB (no cache).
     * A full, frequently-changing list is a poor cache-aside fit (every seat change would
     * have to invalidate it); the point lookup below is what we cache.
     */
    @GetMapping
    public List<SeatEntity> list() {
        return seats.findAll();
    }

    /** GET /api/seats/cache/stats — cache hit/miss counters (mapped before /{seatId}). */
    @GetMapping("/cache/stats")
    public Map<String, Object> cacheStats() {
        return cache.stats();
    }

    /** GET /api/seats/{seatId} — one seat, served cache-aside (Redis → miss → Postgres). */
    @GetMapping("/{seatId}")
    public ResponseEntity<SeatView> get(@PathVariable String seatId) {
        return query.getSeat(seatId)
                .map(ResponseEntity::ok)
                .orElseGet(() -> ResponseEntity.notFound().build());
    }
}
