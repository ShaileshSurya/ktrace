package io.ktrace.spring;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

import jakarta.validation.constraints.Min;

/**
 * Spring Boot configuration properties for ktrace.
 * <p>
 * Binds {@code ktrace.*} properties from application.yml to Java configuration.
 * Spring Boot automatically resolves properties from multiple sources in priority order:
 * <ol>
 *   <li>application.yml / application.properties</li>
 *   <li>Spring Cloud Config / Consul / Vault</li>
 *   <li>System properties ({@code -Dktrace.enabled=true})</li>
 *   <li>Environment variables ({@code KTRACE_ENABLED=true})</li>
 * </ol>
 * <p>
 * Example configuration:
 * <pre>
 * ktrace:
 *   enabled: true                           # Required to activate tracing
 *   application-name: order-service         # Required
 *   bootstrap-servers: localhost:9092       # Optional, falls back to spring.kafka.bootstrap-servers
 *   trace-topic: __ktrace                   # Optional
 *   async-queue-size: 1000                  # Optional
 *   close-timeout-ms: 5000                  # Optional
 *   trace-topic-producer-retries: 0         # Optional
 * </pre>
 *
 * @see KTraceDefaults for default values
 * @see KTraceConfigurer for programmatic configuration
 * @see io.ktrace.core.config.InterceptorConfig
 */
@ConfigurationProperties(prefix = "ktrace")
@Validated
public class KTraceProperties {

    /**
     * Enable or disable ktrace tracing.
     * <p>
     * Default: {@code false} (tracing disabled, opt-in required).
     * <p>
     * Users must explicitly set {@code ktrace.enabled=true} to activate tracing.
     */
    private boolean enabled = KTraceDefaults.ENABLED;

    /**
     * Application name used in TraceEvent records.
     * <p>
     * If not set, falls back to {@code spring.application.name}.
     * If neither is set, application startup fails.
     * <p>
     * This field is critical for identifying which service produced a trace event.
     */
    private String applicationName;

    /**
     * Kafka bootstrap servers for ktrace trace publisher.
     * <p>
     * If not set, falls back to {@code spring.kafka.bootstrap-servers}.
     * If neither is set, application startup fails.
     * <p>
     * Allows using a different Kafka cluster for trace events if needed.
     */
    private String bootstrapServers;

    /**
     * Kafka topic for publishing TraceEvent records.
     * <p>
     * Default: {@value KTraceDefaults#TRACE_TOPIC}
     */
    private String traceTopic = KTraceDefaults.TRACE_TOPIC;

    /**
     * Internal queue size for async trace event publisher.
     * <p>
     * Higher values provide more buffering but use more memory.
     * If the queue fills up, trace events are dropped (logged as warnings).
     * <p>
     * Default: {@value KTraceDefaults#ASYNC_QUEUE_SIZE}
     */
    @Min(value = 1, message = "ktrace.async-queue-size must be at least 1")
    private int asyncQueueSize = KTraceDefaults.ASYNC_QUEUE_SIZE;

    /**
     * Maximum time (in milliseconds) to wait for queue drain during shutdown.
     * <p>
     * Set to 0 for immediate shutdown (drops all queued events).
     * Higher values give more time for events to publish before shutdown.
     * <p>
     * Default: {@value KTraceDefaults#CLOSE_TIMEOUT_MS} (5 seconds)
     */
    @Min(value = 0, message = "ktrace.close-timeout-ms must be non-negative")
    private int closeTimeoutMs = KTraceDefaults.CLOSE_TIMEOUT_MS;

    /**
     * Number of retries for publishing to the trace topic.
     * <p>
     * Set to 0 for fire-and-forget (no retries).
     * Higher values increase reliability but may impact performance.
     * <p>
     * Default: {@value KTraceDefaults#TRACE_TOPIC_PRODUCER_RETRIES} (no retries)
     */
    @Min(value = 0, message = "ktrace.trace-topic-producer-retries must be non-negative")
    private int traceTopicProducerRetries = KTraceDefaults.TRACE_TOPIC_PRODUCER_RETRIES;

    // Getters and Setters

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public String getApplicationName() {
        return applicationName;
    }

    public void setApplicationName(String applicationName) {
        this.applicationName = applicationName;
    }

    public String getBootstrapServers() {
        return bootstrapServers;
    }

    public void setBootstrapServers(String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
    }

    public String getTraceTopic() {
        return traceTopic;
    }

    public void setTraceTopic(String traceTopic) {
        this.traceTopic = traceTopic;
    }

    public int getAsyncQueueSize() {
        return asyncQueueSize;
    }

    public void setAsyncQueueSize(int asyncQueueSize) {
        this.asyncQueueSize = asyncQueueSize;
    }

    public int getCloseTimeoutMs() {
        return closeTimeoutMs;
    }

    public void setCloseTimeoutMs(int closeTimeoutMs) {
        this.closeTimeoutMs = closeTimeoutMs;
    }

    public int getTraceTopicProducerRetries() {
        return traceTopicProducerRetries;
    }

    public void setTraceTopicProducerRetries(int traceTopicProducerRetries) {
        this.traceTopicProducerRetries = traceTopicProducerRetries;
    }
}
