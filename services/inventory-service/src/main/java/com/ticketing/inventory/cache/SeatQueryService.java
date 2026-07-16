package com.ticketing.inventory.cache;

import com.ticketing.inventory.domain.SeatRepository;
import org.springframework.stereotype.Service;

import java.util.Optional;

/**
 * Read-side seat lookups, served <b>cache-aside</b>:
 *
 * <pre>
 *   read(id):
 *     hit  in Redis?      → return it                       (fast path, no DB)
 *     miss?               → load from Postgres              (source of truth)
 *                           populate Redis for next time
 *                           return it
 * </pre>
 *
 * <p>This is the read path only. The reservation write path
 * ({@code ReservationService}) deliberately reads the seat <b>straight from the DB</b>
 * under the Redis lock — never from this cache — because a stale read there would let two
 * orders book the same seat. Rule of thumb: cache-aside is for read-heavy queries that can
 * tolerate brief staleness (bounded by the TTL), not for decisions that must be exact.
 */
@Service
public class SeatQueryService {

    private final SeatRepository seats;
    private final SeatCache cache;

    public SeatQueryService(SeatRepository seats, SeatCache cache) {
        this.seats = seats;
        this.cache = cache;
    }

    /** One seat by id, cache-aside. Empty if the seat doesn't exist. */
    public Optional<SeatView> getSeat(String seatId) {
        Optional<SeatView> cached = cache.read(seatId);
        if (cached.isPresent()) {
            return cached;   // HIT — no DB touch
        }
        // MISS — load the source of truth and backfill the cache for the next reader.
        Optional<SeatView> fromDb = seats.findById(seatId).map(SeatView::from);
        fromDb.ifPresent(cache::write);
        return fromDb;
    }
}
