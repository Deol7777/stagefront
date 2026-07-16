package com.ticketing.inventory.cache;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The Redis side of the seat cache — the primitives behind the cache-aside pattern
 * (read-through-on-miss lives in {@link SeatQueryService}; this class just talks to Redis).
 *
 * <p>Keys are {@code seat:<seatId>} strings holding the seat's JSON. Reuses the same
 * {@link StringRedisTemplate} the distributed lock uses (note 05) — one Redis, two uses:
 * a lock for write contention, a cache for read load.
 *
 * <h2>Why cache-aside (not write-through)</h2>
 * The application reads the cache and, on a miss, loads the DB and populates the cache.
 * Writes don't update the cache — they <b>invalidate</b> it, and the next read repopulates
 * from the source of truth (the DB). This keeps the DB authoritative and the cache simple;
 * a stale entry can only ever survive as long as its TTL.
 *
 * <h2>Invalidate AFTER commit</h2>
 * {@link #evictAfterCommit} is the important subtlety. If we evicted <i>during</i> the
 * transaction (before commit), a concurrent reader could miss, load the seat's OLD
 * committed value from the DB, and repopulate the cache — leaving a stale entry that
 * outlives our change. Deferring the evict to {@code afterCommit} means by the time the
 * cache is cleared, the DB already holds the new value, so the repopulating read is fresh.
 */
@Component
public class SeatCache {

    private static final Logger log = LoggerFactory.getLogger(SeatCache.class);
    private static final String PREFIX = "seat:";

    private final StringRedisTemplate redis;
    private final ObjectMapper mapper;
    private final Duration ttl;

    // Simple observability so the demo (and dashboard) can show the cache working.
    private final AtomicLong hits = new AtomicLong();
    private final AtomicLong misses = new AtomicLong();

    public SeatCache(StringRedisTemplate redis, ObjectMapper mapper,
                     @Value("${seat.cache.ttl-ms:30000}") long ttlMs) {
        this.redis = redis;
        this.mapper = mapper;
        this.ttl = Duration.ofMillis(ttlMs);
    }

    /** Look up a seat in the cache. Empty = miss (also counts a miss); present = hit. */
    public Optional<SeatView> read(String seatId) {
        String json = redis.opsForValue().get(key(seatId));
        if (json == null) {
            misses.incrementAndGet();
            return Optional.empty();
        }
        try {
            SeatView view = mapper.readValue(json, SeatView.class);
            hits.incrementAndGet();
            return Optional.of(view);
        } catch (JsonProcessingException e) {
            // A corrupt/incompatible cached value (e.g. after a shape change). Treat as a
            // miss and drop it so the next read repopulates cleanly.
            log.warn("Discarding unreadable cache entry for {}: {}", seatId, e.getMessage());
            evict(seatId);
            misses.incrementAndGet();
            return Optional.empty();
        }
    }

    /** Populate the cache with a fresh value, expiring after the configured TTL. */
    public void write(SeatView view) {
        try {
            redis.opsForValue().set(key(view.seatId()), mapper.writeValueAsString(view), ttl);
        } catch (JsonProcessingException e) {
            // Serialization should never fail for this flat record; if it does, skip caching
            // rather than fail the request — a cache is best-effort.
            log.warn("Could not cache seat {}: {}", view.seatId(), e.getMessage());
        }
    }

    /** Remove a seat from the cache immediately. */
    public void evict(String seatId) {
        redis.delete(key(seatId));
    }

    /**
     * Evict once the current transaction commits (or immediately if there is no active
     * transaction). See the class note on why post-commit eviction avoids a stale-repopulate
     * race.
     */
    public void evictAfterCommit(String seatId) {
        if (TransactionSynchronizationManager.isSynchronizationActive()) {
            TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
                @Override
                public void afterCommit() {
                    evict(seatId);
                }
            });
        } else {
            evict(seatId);
        }
    }

    /** Hit/miss counts + hit rate, for the stats endpoint and dashboard. */
    public Map<String, Object> stats() {
        long h = hits.get();
        long m = misses.get();
        long total = h + m;
        double rate = total == 0 ? 0.0 : (double) h / total;
        return Map.of("hits", h, "misses", m, "total", total, "hitRate", rate);
    }

    private String key(String seatId) {
        return PREFIX + seatId;
    }
}
