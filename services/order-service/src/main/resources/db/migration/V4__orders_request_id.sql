-- Make placing an order IDEMPOTENT at the HTTP door.
--
-- The problem this fixes: every consumer in this system dedups (processed-keys
-- store), but the entry point did not. `requestId` was accepted and copied into
-- the OrderPlaced event, yet never stored or checked — so a double-clicked
-- button or a retried POST created a second, third, fourth order. Downstream
-- guards (seat lock + status checks) meant only one could win the seat, but the
-- duplicates still became real CANCELLED orders the customer never asked for.
--
-- Nullable: orders written before this migration have no requestId, and a client
-- may legitimately omit one (it then simply gets no dedup protection).
ALTER TABLE orders ADD COLUMN request_id VARCHAR(64);

-- The UNIQUE INDEX is a BACKSTOP (defence in depth), not the primary mechanism.
--
-- "SELECT then INSERT if absent" is check-then-act: on its own, two concurrent
-- requests can both SELECT nothing and both INSERT. OrderService closes that
-- window with a transaction-scoped advisory lock keyed on the requestId, so
-- same-requestId calls run one-at-a-time and the second one sees the first's
-- committed row instead of colliding — no exception, no ERROR log noise.
--
-- This index is what guarantees correctness even if that lock is ever bypassed
-- (a code path that forgets it, a manual INSERT, a future refactor). Belt and
-- suspenders: the app avoids the collision, the constraint makes it impossible.
--
-- Postgres allows multiple NULLs in a unique index, so pre-existing rows and
-- requestId-less orders coexist fine.
CREATE UNIQUE INDEX orders_request_id_uk ON orders (request_id);
