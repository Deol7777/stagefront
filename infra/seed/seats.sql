-- Demo seat inventory for the flash-sale. Run by `make seed`.
--
-- Generates a 5x10 grid (seat-A1 .. seat-E10 = 50 seats) for one demo show, so
-- the saga has plenty to reserve and load/chaos runs don't run out of seats.
--
-- SAFETY: ON CONFLICT DO NOTHING — this only ADDS missing seats. It never resets
-- an existing seat, so a seat currently RESERVED or SOLD to a live order is left
-- exactly as it is. Resetting owned seats orphans confirmed orders (the invariant
-- checker would flag CONFIRMED_ORDER_WITHOUT_SOLD_SEAT); seeding must never do
-- that. Re-running is therefore always safe.
INSERT INTO seats (seat_id, event_schedule_id, status, updated_at)
SELECT
    'seat-' || chr(64 + row) || num,   -- chr(65)='A' -> seat-A1, seat-A2, ...
    'show-1',
    'AVAILABLE',
    now()
FROM generate_series(1, 5)  AS row,   -- rows A..E
     generate_series(1, 10) AS num    -- numbers 1..10
ON CONFLICT (seat_id) DO NOTHING;
