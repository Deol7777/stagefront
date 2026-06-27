package com.ticketing.inventory.lock;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.List;
import java.util.UUID;

/**
 * A Redis distributed lock for seat contention. When thousands of buyers race
 * for the same seat across many service instances, an in-process lock is useless
 * (each instance has its own). A lock in Redis — shared by all instances — gives
 * real mutual exclusion: only the holder may reserve the seat.
 *
 * <p>How it works:
 * <ul>
 *   <li><b>acquire</b> = {@code SET key token NX PX ttl} — set only if absent
 *       ({@code NX}), with an expiry ({@code PX}). Atomic: exactly one caller wins.</li>
 *   <li><b>ttl</b> = safety net. If the holder crashes without releasing, the key
 *       expires so the seat isn't locked forever.</li>
 *   <li><b>release</b> = delete the key ONLY if it still holds OUR token, done as
 *       a Lua script so check-and-delete is atomic. Stops us from deleting a lock
 *       that already expired and was re-acquired by someone else.</li>
 * </ul>
 */
@Component
public class RedisSeatLock {

    // Atomic compare-and-delete: delete the key only if its value is our token.
    private static final DefaultRedisScript<Long> UNLOCK = new DefaultRedisScript<>(
            "if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('del', KEYS[1]) else return 0 end",
            Long.class);

    private final StringRedisTemplate redis;

    public RedisSeatLock(StringRedisTemplate redis) {
        this.redis = redis;
    }

    /**
     * Try to acquire the lock for {@code key}.
     *
     * @return a unique token if acquired (pass it to {@link #release}), or
     *         {@code null} if someone else already holds it.
     */
    public String tryAcquire(String key, Duration ttl) {
        String token = UUID.randomUUID().toString();
        Boolean ok = redis.opsForValue().setIfAbsent(key, token, ttl);   // SET NX PX
        return Boolean.TRUE.equals(ok) ? token : null;
    }

    /** Release the lock only if we still hold it (token matches). */
    public void release(String key, String token) {
        redis.execute(UNLOCK, List.of(key), token);
    }
}
