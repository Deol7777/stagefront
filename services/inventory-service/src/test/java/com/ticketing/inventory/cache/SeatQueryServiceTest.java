package com.ticketing.inventory.cache;

import com.ticketing.inventory.domain.SeatEntity;
import com.ticketing.inventory.domain.SeatRepository;
import com.ticketing.inventory.domain.SeatStatus;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

/**
 * Unit tests for the cache-aside read logic. The cache and repository are mocked, so no
 * Redis or DB is involved — we only assert the orchestration: hit skips the DB, miss loads
 * the DB and backfills the cache.
 */
@ExtendWith(MockitoExtension.class)
class SeatQueryServiceTest {

    @Mock SeatRepository seats;
    @Mock SeatCache cache;

    private SeatQueryService service() {
        return new SeatQueryService(seats, cache);
    }

    @Test
    void cacheHitSkipsTheDatabase() {
        var cached = new SeatView("seat-1", "show-9", "AVAILABLE", null, null);
        when(cache.read("seat-1")).thenReturn(Optional.of(cached));

        Optional<SeatView> result = service().getSeat("seat-1");

        assertEquals(cached, result.orElseThrow());
        // The whole point of a hit: the DB is never touched, and we don't re-write the cache.
        verifyNoInteractions(seats);
        verify(cache, never()).write(cached);
    }

    @Test
    void cacheMissLoadsDatabaseAndBackfillsCache() {
        when(cache.read("seat-2")).thenReturn(Optional.empty());

        SeatEntity row = mock(SeatEntity.class);
        when(row.getSeatId()).thenReturn("seat-2");
        when(row.getEventScheduleId()).thenReturn("show-9");
        when(row.getStatus()).thenReturn(SeatStatus.AVAILABLE);
        when(seats.findById("seat-2")).thenReturn(Optional.of(row));

        Optional<SeatView> result = service().getSeat("seat-2");

        assertTrue(result.isPresent());
        assertEquals("seat-2", result.get().seatId());
        // Miss → we load the DB and backfill the cache for the next reader.
        verify(seats).findById("seat-2");
        verify(cache).write(result.get());
    }

    @Test
    void missingSeatReturnsEmptyAndCachesNothing() {
        when(cache.read("nope")).thenReturn(Optional.empty());
        when(seats.findById("nope")).thenReturn(Optional.empty());

        Optional<SeatView> result = service().getSeat("nope");

        assertTrue(result.isEmpty());
        verify(cache, never()).write(org.mockito.ArgumentMatchers.any());
    }
}
