package io.ktrace.core.context;

import io.ktrace.core.config.KTraceConfig;
import org.slf4j.MDC;

/**
 * Manages ktrace context in SLF4J MDC (Mapped Diagnostic Context).
 * <p>
 * This class ensures trace IDs are automatically included in application logs
 * while maintaining strict lifecycle management to prevent MDC pollution and
 * ThreadLocal memory leaks.
 * <p>
 * MDC keys set by ktrace:
 * <ul>
 *   <li>{@code ktrace.traceId} - top-level trace UUID</li>
 *   <li>{@code ktrace.spanId} - current span UUID</li>
 *   <li>{@code ktrace.parentSpanId} - parent span UUID (absent for root spans)</li>
 * </ul>
 * <p>
 * CRITICAL: {@link #clear()} removes ONLY {@code ktrace.*} keys. It never calls
 * {@code MDC.clear()} which would wipe user/framework keys like {@code requestId},
 * {@code userId}, {@code sessionId}, etc.
 *
 * @see TraceContext for trace state management
 * @see TraceContextPropagator for Kafka header propagation
 */
public final class KTraceMDC {

    public static final String TRACE_ID_KEY = "ktrace.traceId";
    public static final String SPAN_ID_KEY = "ktrace.spanId";
    public static final String PARENT_SPAN_ID_KEY = "ktrace.parentSpanId";

    private KTraceMDC() {
    }

    /**
     * Sets ktrace MDC keys from the given context (if logging is enabled).
     * <p>
     * This sets 2-3 MDC keys IF logging is enabled:
     * <ul>
     *   <li>{@code ktrace.traceId} - always set</li>
     *   <li>{@code ktrace.spanId} - always set</li>
     *   <li>{@code ktrace.parentSpanId} - set only if context has a parent</li>
     * </ul>
     * <p>
     * If logging is disabled via config, this method does nothing.
     * After calling this (when enabled), application logs will automatically include these keys.
     *
     * @param context the trace context to set (must not be null)
     * @throws NullPointerException if context is null
     */
    public static void put(TraceContext context) {
        if (context == null) {
            throw new NullPointerException("context must not be null");
        }

        if (!KTraceConfig.getInstance().isLoggingEnabled()) {
            return;
        }

        MDC.put(TRACE_ID_KEY, context.getTraceId());
        MDC.put(SPAN_ID_KEY, context.getSpanId());

        if (context.getParentSpanId() != null) {
            MDC.put(PARENT_SPAN_ID_KEY, context.getParentSpanId());
        } else {
            MDC.remove(PARENT_SPAN_ID_KEY);
        }
    }

    /**
     * Removes ktrace MDC keys SELECTIVELY (if logging is enabled).
     * <p>
     * This removes ONLY {@code ktrace.*} keys, preserving all other MDC keys
     * set by the application or other frameworks (e.g., {@code requestId},
     * {@code userId}, {@code sessionId}).
     * <p>
     * If logging is disabled, this method does nothing (keys were never set).
     * <p>
     * IMPORTANT: This must be called in a finally block to prevent ThreadLocal
     * memory leaks in SLF4J's MDC implementation.
     * <p>
     * Example:
     * <pre>
     * try {
     *     KTraceMDC.put(context);
     *     // ... process message
     * } finally {
     *     KTraceMDC.clear();
     * }
     * </pre>
     */
    public static void clear() {
        if (!KTraceConfig.getInstance().isLoggingEnabled()) {
            return;
        }

        MDC.remove(TRACE_ID_KEY);
        MDC.remove(SPAN_ID_KEY);
        MDC.remove(PARENT_SPAN_ID_KEY);
    }

    /**
     * Reads the current trace context from MDC.
     * <p>
     * This is useful for cross-thread hand-off when ThreadLocal context is lost:
     * <pre>
     * TraceContext ctx = KTraceMDC.current();
     * executor.submit(() -> {
     *     KTraceMDC.put(ctx);
     *     // ... work with trace context
     *     KTraceMDC.clear();
     * });
     * </pre>
     *
     * @return the current context from MDC, or null if not set
     */
    public static TraceContext current() {
        String traceId = MDC.get(TRACE_ID_KEY);
        if (traceId == null) {
            return null;
        }

        String spanId = MDC.get(SPAN_ID_KEY);
        if (spanId == null) {
            return null;
        }

        String parentSpanId = MDC.get(PARENT_SPAN_ID_KEY);
        return new TraceContext(traceId, spanId, parentSpanId);
    }

    /**
     * Executes a Runnable with trace context in MDC, automatically cleaning up.
     * <p>
     * This ensures MDC is cleared even if the Runnable throws an exception.
     * Useful for reactive/async code where explicit try-finally is verbose.
     * <p>
     * Example:
     * <pre>
     * KTraceMDC.scoped(context, () -> {
     *     log.info("Inside scoped block");  // ktrace.* keys present
     * });
     * // ktrace.* keys automatically cleared
     * </pre>
     *
     * @param context  the trace context to set (must not be null)
     * @param runnable the code to execute with trace context
     * @throws NullPointerException if context or runnable is null
     */
    public static void scoped(TraceContext context, Runnable runnable) {
        if (context == null) {
            throw new NullPointerException("context must not be null");
        }
        if (runnable == null) {
            throw new NullPointerException("runnable must not be null");
        }

        try {
            put(context);
            runnable.run();
        } finally {
            clear();
        }
    }
}
