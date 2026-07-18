-- Carry the W3C trace context through the outbox.
-- Full rationale: order-service V3__outbox_trace_parent.sql.
--
-- Short version: the relay publishes on a timer thread with no trace context, so
-- without persisting it the saga shatters into unrelated single-span traces.
-- NULLable: pre-migration rows and events produced outside a trace are valid.
ALTER TABLE outbox ADD COLUMN trace_parent VARCHAR(64);
