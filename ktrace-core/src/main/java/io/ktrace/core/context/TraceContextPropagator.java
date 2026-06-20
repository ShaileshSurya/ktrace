package io.ktrace.core.context;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;

import java.nio.charset.StandardCharsets;

/**
 * Propagates trace context via Kafka message headers.
 * <p>
 * This class bridges between {@link TraceContext} objects and Kafka headers,
 * enabling zero-config causality tracing across services.
 * <p>
 * Kafka headers used (all with {@code ktrace-} prefix):
 * <ul>
 *   <li>{@code ktrace-trace-id} - top-level trace UUID (required)</li>
 *   <li>{@code ktrace-span-id} - current span UUID (required)</li>
 *   <li>{@code ktrace-parent-span-id} - parent span UUID (optional, absent for root)</li>
 * </ul>
 * <p>
 * All header values are UTF-8 encoded UUIDs in lowercase with hyphens
 * (e.g., "550e8400-e29b-41d4-a716-446655440000").
 *
 * @see TraceContext for trace state management
 * @see KTraceMDC for MDC integration
 */
public final class TraceContextPropagator {

    public static final String HEADER_TRACE_ID = "ktrace-trace-id";
    public static final String HEADER_SPAN_ID = "ktrace-span-id";
    public static final String HEADER_PARENT_SPAN_ID = "ktrace-parent-span-id";

    private TraceContextPropagator() {
    }

    /**
     * Extracts trace context from Kafka headers.
     * <p>
     * This reads {@code ktrace-trace-id} and {@code ktrace-span-id} headers.
     * If both are present, returns a TraceContext. If either is missing, returns null.
     *
     * @param headers the Kafka message headers
     * @return the extracted context, or null if headers are missing
     */
    public static TraceContext extract(Headers headers) {
        if (headers == null) {
            return null;
        }

        Header traceIdHeader = headers.lastHeader(HEADER_TRACE_ID);
        Header spanIdHeader = headers.lastHeader(HEADER_SPAN_ID);

        if (traceIdHeader == null || spanIdHeader == null) {
            return null;
        }

        String traceId = new String(traceIdHeader.value(), StandardCharsets.UTF_8);
        String spanId = new String(spanIdHeader.value(), StandardCharsets.UTF_8);

        Header parentSpanIdHeader = headers.lastHeader(HEADER_PARENT_SPAN_ID);
        String parentSpanId = null;
        if (parentSpanIdHeader != null) {
            parentSpanId = new String(parentSpanIdHeader.value(), StandardCharsets.UTF_8);
        }

        return new TraceContext(traceId, spanId, parentSpanId);
    }

    /**
     * Injects trace context into Kafka headers.
     * <p>
     * This writes:
     * <ul>
     *   <li>{@code ktrace-trace-id} - always</li>
     *   <li>{@code ktrace-span-id} - always</li>
     *   <li>{@code ktrace-parent-span-id} - only if context has a parent</li>
     * </ul>
     * <p>
     * Existing ktrace headers are NOT removed (Kafka allows duplicate headers).
     * The producer interceptor should call this on the ProducerRecord before sending.
     *
     * @param context the trace context to inject (must not be null)
     * @param headers the Kafka message headers to modify
     * @throws NullPointerException if context or headers is null
     */
    public static void inject(TraceContext context, Headers headers) {
        if (context == null) {
            throw new NullPointerException("context must not be null");
        }
        if (headers == null) {
            throw new NullPointerException("headers must not be null");
        }

        headers.add(HEADER_TRACE_ID, context.getTraceId().getBytes(StandardCharsets.UTF_8));
        headers.add(HEADER_SPAN_ID, context.getSpanId().getBytes(StandardCharsets.UTF_8));

        if (context.getParentSpanId() != null) {
            headers.add(HEADER_PARENT_SPAN_ID, context.getParentSpanId().getBytes(StandardCharsets.UTF_8));
        }
    }

    /**
     * Extracts trace context from a ConsumerRecord and binds it to ThreadLocal + MDC.
     * <p>
     * This is a convenience method for consumers that combines:
     * <ol>
     *   <li>Extract context from headers via {@link #extract(Headers)}</li>
     *   <li>Store in ThreadLocal via {@link TraceContext#setCurrent(TraceContext)}</li>
     *   <li>Set MDC via {@link KTraceMDC#put(TraceContext)}</li>
     * </ol>
     * <p>
     * If no ktrace headers are present, this does nothing (no-op).
     * <p>
     * Example consumer usage:
     * <pre>
     * try {
     *     TraceContextPropagator.extractAndBind(record);
     *     // ... process message (logs include ktrace.* keys)
     * } finally {
     *     KTraceMDC.clear();
     *     TraceContext.clearCurrent();
     * }
     * </pre>
     *
     * @param record the consumer record to extract context from
     */
    public static void extractAndBind(ConsumerRecord<?, ?> record) {
        if (record == null) {
            return;
        }

        TraceContext context = extract(record.headers());
        if (context != null) {
            TraceContext.setCurrent(context);
            KTraceMDC.put(context);
        }
    }

    /**
     * Extracts trace context from a ConsumerRecord or creates a root context.
     * <p>
     * This is a convenience method that:
     * <ol>
     *   <li>Tries to extract context from headers</li>
     *   <li>If headers don't have ktrace context: creates new root context</li>
     *   <li>Stores context in ThreadLocal via {@link TraceContext#setCurrent(TraceContext)}</li>
     *   <li>Sets MDC via {@link KTraceMDC#put(TraceContext)}</li>
     * </ol>
     * <p>
     * This ensures every consumer has trace context:
     * - Message with headers: uses parent's trace ID (child span created)
     * - Message without headers: creates new root (new trace chain)
     * <p>
     * IMPORTANT: Caller must ensure cleanup in finally block:
     * <pre>
     * try {
     *     TraceContextPropagator.extractOrCreateAndBind(record);
     *     // ... process message
     * } finally {
     *     KTraceMDC.clear();
     *     TraceContext.clearCurrent();
     * }
     * </pre>
     *
     * @param record the consumer record to extract from or create root context
     */
    public static void extractOrCreateAndBind(ConsumerRecord<?, ?> record) {
        if (record == null) {
            return;
        }

        TraceContext context = extract(record.headers());
        if (context == null) {
            // No ktrace headers: create root context
            context = TraceContext.currentOrCreateRoot();
        } else {
            // Has ktrace headers: use extracted context
            TraceContext.setCurrent(context);
        }

        KTraceMDC.put(context);
    }
}
