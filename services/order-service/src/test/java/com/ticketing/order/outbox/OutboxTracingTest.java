package com.ticketing.order.outbox;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.ticketing.contracts.Money;
import com.ticketing.contracts.events.EventType;
import com.ticketing.contracts.events.OrderPlaced;
import io.micrometer.tracing.CurrentTraceContext;
import io.micrometer.tracing.Span;
import io.micrometer.tracing.TraceContext;
import io.micrometer.tracing.Tracer;
import io.micrometer.tracing.propagation.Propagator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Regression tests for trace-context propagation THROUGH the outbox.
 *
 * <p>Why these exist: the outbox decouples writing an event from publishing it,
 * which means the publish happens on a scheduler thread with no trace context.
 * If the capture-on-write / restore-on-publish pair is ever removed, tracing does
 * not break loudly — records still get published, consumers still work, and the
 * only symptom is that Jaeger quietly shows one saga as a dozen unrelated traces.
 * A silent failure needs a test.
 *
 * <p>Scope, honestly stated: these verify the WIRING (the context is captured,
 * persisted, and re-established around the send). They do not prove W3C
 * propagation itself is correct — that is what the end-to-end run against real
 * Kafka + Jaeger proves. See docs/notes/15-observability-tracing.md.
 */
@ExtendWith(MockitoExtension.class)
class OutboxTracingTest {

    private static final String TRACEPARENT =
            "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";

    @Mock OutboxRepository repo;
    @Mock Tracer tracer;
    @Mock Propagator propagator;
    @Mock CurrentTraceContext currentTraceContext;
    @Mock TraceContext traceContext;

    // ---- write side -------------------------------------------------------

    @Test
    void captures_the_active_trace_context_onto_the_row() {
        // A trace IS active (we're inside the HTTP request that placed the order).
        when(tracer.currentTraceContext()).thenReturn(currentTraceContext);
        when(currentTraceContext.context()).thenReturn(traceContext);
        when(traceContext.traceId()).thenReturn("4bf92f3577b34da6a3ce929d0e0e4736");
        // Real propagators write into the carrier via the setter; imitate that.
        doAnswer(inv -> {
            Map<String, String> carrier = inv.getArgument(1);
            carrier.put("traceparent", TRACEPARENT);
            return null;
        }).when(propagator).inject(eq(traceContext), any(), any());

        writer().append(EventType.ORDER_PLACED, "Order", "order-1", placed(), null);

        OutboxEntity saved = savedRow();
        assertEquals(TRACEPARENT, saved.getTraceParent(),
                "traceparent must be persisted so the relay can restore it later");
    }

    @Test
    void stores_null_when_no_trace_is_active() {
        // No ambient trace — e.g. a background job, or tracing switched off.
        // Must degrade to an untraced publish, not blow up the business write.
        when(tracer.currentTraceContext()).thenReturn(currentTraceContext);
        when(currentTraceContext.context()).thenReturn(null);

        writer().append(EventType.ORDER_PLACED, "Order", "order-1", placed(), null);

        assertNull(savedRow().getTraceParent());
        verify(propagator, never()).inject(any(), any(), any());
    }

    // ---- publish side -----------------------------------------------------

    @Test
    void relay_publishes_inside_the_restored_trace() {
        Span span = mock(Span.class);
        Span.Builder builder = mock(Span.Builder.class);
        when(propagator.extract(any(), any())).thenReturn(builder);
        when(builder.name(anyString())).thenReturn(builder);
        when(builder.tag(anyString(), anyString())).thenReturn(builder);
        when(builder.start()).thenReturn(span);
        when(tracer.withSpan(span)).thenReturn(mock(Tracer.SpanInScope.class));

        var kafka = kafkaTemplate();
        relay(kafka).publishPending();

        // The restored span must WRAP the send: if the scope were opened after
        // (or not at all) the KafkaTemplate would inject the scheduler's empty
        // context and the trace would be severed at this hop.
        verify(tracer).withSpan(span);
        verify(kafka).send(anyString(), anyString(), anyString());
        verify(span).end();
    }

    @Test
    void relay_still_publishes_rows_that_have_no_trace_context() {
        // Rows written before the trace_parent column existed must not get stuck
        // in the outbox forever — publish them untraced.
        var kafka = kafkaTemplate();
        relay(kafka, row(null)).publishPending();

        verify(kafka).send(anyString(), anyString(), anyString());
        verify(propagator, never()).extract(any(), any());
    }

    // ---- helpers ----------------------------------------------------------

    private OutboxWriter writer() {
        // JavaTimeModule: the envelope carries an Instant occurredAt, which a bare
        // ObjectMapper refuses to serialize. Boot registers this for us at runtime.
        var mapper = new ObjectMapper().registerModule(new JavaTimeModule());
        return new OutboxWriter(repo, mapper, tracer, propagator);
    }

    private OutboxRelay relay(org.springframework.kafka.core.KafkaTemplate<String, String> kafka) {
        return relay(kafka, row(TRACEPARENT));
    }

    private OutboxRelay relay(org.springframework.kafka.core.KafkaTemplate<String, String> kafka,
                              OutboxEntity pending) {
        when(repo.findByPublishedFalseOrderByCreatedAtAsc(any())).thenReturn(List.of(pending));
        return new OutboxRelay(repo, kafka, tracer, propagator, 100);
    }

    @SuppressWarnings("unchecked")
    private org.springframework.kafka.core.KafkaTemplate<String, String> kafkaTemplate() {
        var kafka = mock(org.springframework.kafka.core.KafkaTemplate.class);
        when(kafka.send(anyString(), anyString(), anyString()))
                .thenReturn(CompletableFuture.completedFuture(null));
        return kafka;
    }

    private OutboxEntity row(String traceParent) {
        return new OutboxEntity(UUID.randomUUID(), "Order", "order-1", "OrderPlaced",
                "orders.placed", "order-1", "{}", 1, Instant.now(), traceParent);
    }

    private OutboxEntity savedRow() {
        ArgumentCaptor<OutboxEntity> captor = ArgumentCaptor.forClass(OutboxEntity.class);
        verify(repo).save(captor.capture());
        return captor.getValue();
    }

    private OrderPlaced placed() {
        return new OrderPlaced("order-1", "user-1", "req-1", "seat-1", "show-1",
                new Money(new BigDecimal("50.00"), "USD"));
    }

    private static <T> T mock(Class<T> type) {
        return org.mockito.Mockito.mock(type);
    }
}
