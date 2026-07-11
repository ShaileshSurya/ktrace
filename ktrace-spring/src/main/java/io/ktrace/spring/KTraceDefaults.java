package io.ktrace.spring;

/**
 * Default values for ktrace configuration properties.
 * <p>
 * Centralized constants to avoid hardcoding defaults throughout the codebase.
 * These defaults match ktrace-core's behavior for consistency.
 * <p>
 * NOTE: All ktrace properties can be set via:
 * <ul>
 *   <li>application.yml: {@code ktrace.enabled=true}</li>
 *   <li>System property: {@code -Dktrace.enabled=true}</li>
 *   <li>Environment variable: {@code KTRACE_ENABLED=true}</li>
 *   <li>Programmatic: {@link KTraceConfigurer}</li>
 * </ul>
 *
 * @see KTraceProperties
 * @see io.ktrace.core.config.InterceptorConfig
 */
public final class KTraceDefaults {

    private KTraceDefaults() {
        // Prevent instantiation
    }

    /**
     * Default: tracing is DISABLED unless explicitly enabled.
     * Users must set {@code ktrace.enabled=true} to activate tracing.
     * <p>
     * Can be set via application.yml, system property, or environment variable.
     */
    public static final boolean ENABLED = false;

    /**
     * Default Kafka topic for publishing TraceEvent records.
     * Must start with double underscore (internal topic convention).
     */
    public static final String TRACE_TOPIC = "__ktrace";

    /**
     * Default internal queue size for async trace event publisher.
     * Higher values = more buffering but more memory usage.
     */
    public static final int ASYNC_QUEUE_SIZE = 1000;

    /**
     * Default maximum time (in milliseconds) to wait for queue drain during shutdown.
     * Set to 0 for immediate shutdown (drops all queued events).
     */
    public static final int CLOSE_TIMEOUT_MS = 5000;

    /**
     * Default number of retries for publishing to the trace topic.
     * Set to 0 = fire-and-forget (no retries).
     */
    public static final int TRACE_TOPIC_PRODUCER_RETRIES = 0;
}
