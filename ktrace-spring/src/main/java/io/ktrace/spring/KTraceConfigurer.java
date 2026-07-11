package io.ktrace.spring;

/**
 * Programmatic configuration for ktrace.
 * <p>
 * Optional - only needed if you want to configure ktrace programmatically
 * instead of via properties. Uses fluent builder pattern for easy configuration.
 * <p>
 * Example usage:
 * <pre>
 * &#64;Configuration
 * public class KTraceConfig {
 *
 *     &#64;Bean
 *     public KTraceConfigurer ktraceConfigurer() {
 *         return new KTraceConfigurer()
 *             .enabled(true)
 *             .bootstrapServers("kafka:9092")
 *             .applicationName("order-service")
 *             .asyncQueueSize(2000);
 *     }
 * }
 * </pre>
 * <p>
 * <b>Priority:</b> Properties from {@link KTraceProperties} take precedence over
 * programmatic configuration. Use this when you cannot use application.yml
 * (e.g., dynamic configuration, testing, or non-Spring Boot environments).
 *
 * @see KTraceProperties for property-based configuration
 * @see KTraceDefaults for default values
 */
public class KTraceConfigurer {

    private Boolean enabled;
    private String applicationName;
    private String bootstrapServers;
    private String traceTopic;
    private Integer asyncQueueSize;
    private Integer closeTimeoutMs;
    private Integer traceTopicProducerRetries;

    /**
     * Enable or disable ktrace tracing.
     *
     * @param enabled true to enable tracing, false to disable
     * @return this configurer for method chaining
     */
    public KTraceConfigurer enabled(boolean enabled) {
        this.enabled = enabled;
        return this;
    }

    /**
     * Set the application name used in TraceEvent records.
     *
     * @param applicationName the application name
     * @return this configurer for method chaining
     */
    public KTraceConfigurer applicationName(String applicationName) {
        this.applicationName = applicationName;
        return this;
    }

    /**
     * Set the Kafka bootstrap servers for ktrace trace publisher.
     *
     * @param bootstrapServers comma-separated list of host:port
     * @return this configurer for method chaining
     */
    public KTraceConfigurer bootstrapServers(String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
        return this;
    }

    /**
     * Set the Kafka topic for publishing TraceEvent records.
     *
     * @param traceTopic the trace topic name
     * @return this configurer for method chaining
     */
    public KTraceConfigurer traceTopic(String traceTopic) {
        this.traceTopic = traceTopic;
        return this;
    }

    /**
     * Set the internal queue size for async trace event publisher.
     *
     * @param asyncQueueSize queue size (must be positive)
     * @return this configurer for method chaining
     */
    public KTraceConfigurer asyncQueueSize(int asyncQueueSize) {
        this.asyncQueueSize = asyncQueueSize;
        return this;
    }

    /**
     * Set the maximum time (in milliseconds) to wait for queue drain during shutdown.
     *
     * @param closeTimeoutMs timeout in milliseconds (0 for immediate shutdown)
     * @return this configurer for method chaining
     */
    public KTraceConfigurer closeTimeoutMs(int closeTimeoutMs) {
        this.closeTimeoutMs = closeTimeoutMs;
        return this;
    }

    /**
     * Set the number of retries for publishing to the trace topic.
     *
     * @param retries retry count (0 for fire-and-forget)
     * @return this configurer for method chaining
     */
    public KTraceConfigurer traceTopicProducerRetries(int retries) {
        this.traceTopicProducerRetries = retries;
        return this;
    }

    // Package-private getters for auto-configuration

    Boolean getEnabled() {
        return enabled;
    }

    String getApplicationName() {
        return applicationName;
    }

    String getBootstrapServers() {
        return bootstrapServers;
    }

    String getTraceTopic() {
        return traceTopic;
    }

    Integer getAsyncQueueSize() {
        return asyncQueueSize;
    }

    Integer getCloseTimeoutMs() {
        return closeTimeoutMs;
    }

    Integer getTraceTopicProducerRetries() {
        return traceTopicProducerRetries;
    }
}
