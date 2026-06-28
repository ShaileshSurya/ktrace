package io.ktrace.core.interceptor;

import io.ktrace.core.config.InterceptorConfig;
import io.ktrace.core.context.TraceContext;
import io.ktrace.core.context.TraceContextPropagator;
import io.ktrace.core.event.TraceEvent;
import io.ktrace.core.publisher.AsyncTracePublisher;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.Headers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.UUID;

/**
 * Kafka producer interceptor for zero-config causality tracing.
 * <p>
 * This interceptor wraps every {@code KafkaProducer.send()} call to:
 * <ul>
 *   <li>Extract or generate trace context (traceId, spanId, parentSpanId)</li>
 *   <li>Inject {@code ktrace-*} headers into the producer record</li>
 *   <li>Publish a {@link io.ktrace.core.event.TraceEvent} to the {@code __ktrace} topic</li>
 * </ul>
 * <p>
 * Registration (application producer config):
 * <pre>
 * interceptor.classes=io.ktrace.core.interceptor.KTraceProducerInterceptor
 * ktrace.enabled=true
 * ktrace.trace-topic=__ktrace
 * </pre>
 * <p>
 * Thread safety: {@link #onSend(ProducerRecord)} is thread-safe via ThreadLocal isolation.
 * The interceptor does NOT modify ThreadLocal or MDC (read-only operation).
 * <p>
 * Error handling: All methods use graceful degradation. If tracing fails, the application
 * send continues normally. This interceptor never throws exceptions.
 *
 * @param <K> producer record key type
 * @param <V> producer record value type
 * @see io.ktrace.core.context.TraceContext
 * @see io.ktrace.core.context.TraceContextPropagator
 */
public class KTraceProducerInterceptor<K, V> implements ProducerInterceptor<K, V> {

    private static final Logger log = LoggerFactory.getLogger(KTraceProducerInterceptor.class);

    private InterceptorConfig config;
    private AsyncTracePublisher publisher;
    private boolean publisherAvailable;

    /**
     * Configures the interceptor and initializes the async trace publisher.
     * <p>
     * Called once when the KafkaProducer is instantiated. Parses ktrace configuration
     * and creates an internal trace publisher. If initialization fails, tracing is
     * disabled but the application producer continues to work (graceful degradation).
     *
     * @param configs producer configuration map (contains ktrace.* properties)
     */
    @Override
    public void configure(Map<String, ?> configs) {
        this.config = InterceptorConfig.fromProducerConfig(configs);

        // Fast path: tracing disabled
        if (!config.isEnabled()) {
            log.debug("ktrace disabled via configuration");
            this.publisherAvailable = false;
            return;
        }

        try {
            // Extract bootstrap.servers from producer config
            String bootstrapServers = (String) configs.get(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG);
            if (bootstrapServers == null || bootstrapServers.isEmpty()) {
                log.error("ktrace cannot initialize: bootstrap.servers not found in producer config");
                this.publisherAvailable = false;
                return;
            }

            // Initialize async publisher
            this.publisher = new AsyncTracePublisher(
                    config.getTraceTopic(),
                    bootstrapServers,
                    config.getAsyncQueueSize(),
                    config.getCloseTimeoutMs(),
                    config.getPublisherRetries()
            );
            this.publisherAvailable = true;

            log.info("ktrace initialized: traceTopic={}, queueSize={}, retries={}",
                    config.getTraceTopic(), config.getAsyncQueueSize(), config.getPublisherRetries());

        } catch (Exception e) {
            log.error("Failed to initialize ktrace publisher, tracing disabled. Cause: {}", e.getMessage(), e);
            this.publisherAvailable = false;
        }
    }

    /**
     * Intercepts producer send and injects trace context.
     * <p>
     * This method implements the core tracing logic:
     * <ol>
     *   <li>Check for existing context (ThreadLocal or headers)</li>
     *   <li>Generate new trace context (root or child span)</li>
     *   <li>Inject ktrace headers into the record</li>
     *   <li>Build and publish TraceEvent to __ktrace topic</li>
     * </ol>
     * <p>
     * IMPORTANT: This method does NOT modify ThreadLocal or MDC (read-only).
     * The trace context is created locally and injected into headers only.
     *
     * @param record the producer record being sent
     * @return the modified record with ktrace headers injected
     */
    @Override
    public ProducerRecord<K, V> onSend(ProducerRecord<K, V> record) {
        // Fast path: tracing disabled
        if (!config.isEnabled()) {
            return record;
        }

        try {
            // Step 1-3: Check for existing context (ThreadLocal or headers)
            TraceContext context = TraceContext.current();
            if (context == null) {
                context = TraceContextPropagator.extract(record.headers());
            }

            // Step 4-6: Generate new context
            String traceId;
            String spanId = UUID.randomUUID().toString();
            String parentSpanId = null;

            if (context == null) {
                // ROOT SPAN: No existing context (Scenario 1)
                traceId = UUID.randomUUID().toString();
                parentSpanId = null;
            } else {
                // CHILD SPAN: Reuse traceId, set parentSpanId (Scenario 2, future)
                traceId = context.getTraceId();
                parentSpanId = context.getSpanId();
            }

            // Step 7: Create local context (do NOT modify ThreadLocal)
            TraceContext newContext = new TraceContext(traceId, spanId, parentSpanId);

            // Step 8-9: Remove old headers and inject new ones (idempotent)
            removeKtraceHeaders(record.headers());
            TraceContextPropagator.inject(newContext, record.headers());

            // Step 10: Build TraceEvent
            TraceEvent event = buildTraceEvent(newContext, record);

            // Step 11: Publish (non-blocking, fire-and-forget)
            if (publisherAvailable && publisher != null) {
                publisher.publish(event);
            } else {
                log.debug("ktrace publisher unavailable, skipping trace event");
            }

            // Step 12: Return modified record
            return record;

        } catch (Exception e) {
            // Never fail the application send
            log.error("ktrace onSend failed, returning original record. Cause: {}", e.getMessage(), e);
            return record;
        }
    }

    /**
     * Called when broker acknowledges the send (or fails).
     * <p>
     * This is a no-op for now. Future enhancement: update TraceEvent with
     * actual partition/offset after acknowledgement.
     *
     * @param metadata record metadata (partition, offset, timestamp)
     * @param exception send exception (null if successful)
     */
    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
        // No-op for Scenario 1
        // Future: could publish updated TraceEvent with actual partition/offset
    }

    /**
     * Closes the interceptor and releases resources.
     * <p>
     * Called when the producer is closed. Attempts to drain the async publisher's
     * queue for up to {@code ktrace.close-timeout-ms} milliseconds. If the timeout
     * expires, remaining events are dropped and a warning is logged.
     * <p>
     * This method blocks the calling thread during shutdown (acceptable for cleanup).
     */
    @Override
    public void close() {
        if (publisher != null) {
            try {
                publisher.close();
            } catch (Exception e) {
                log.error("Failed to close ktrace publisher", e);
            }
        }
    }

    /**
     * Removes existing ktrace headers from the record (idempotency).
     * <p>
     * This ensures the interceptor is authoritative - existing headers are
     * replaced with fresh context. Prevents duplicate headers.
     *
     * @param headers the record headers to clean
     */
    private void removeKtraceHeaders(Headers headers) {
        headers.remove(TraceContextPropagator.HEADER_TRACE_ID);
        headers.remove(TraceContextPropagator.HEADER_SPAN_ID);
        headers.remove(TraceContextPropagator.HEADER_PARENT_SPAN_ID);
    }

    /**
     * Builds a TraceEvent from trace context and producer record.
     * <p>
     * Extracts trigger metadata from ThreadLocal (if available). Does NOT
     * access the record key or value (per spec).
     *
     * @param context the trace context (traceId, spanId, parentSpanId)
     * @param record the producer record being sent
     * @return a complete TraceEvent ready for publishing
     */
    private TraceEvent buildTraceEvent(TraceContext context, ProducerRecord<K, V> record) {
        // Get trigger metadata (null for Scenario 1, set in Scenario 2)
        TraceContext.TriggerMetadata trigger = TraceContext.getTrigger();

        return TraceEvent.builder()
                .traceId(context.getTraceId())
                .spanId(context.getSpanId())
                .parentSpanId(context.getParentSpanId())
                .producedTopic(record.topic())
                .producedPartition(-1)  // Not yet known
                .producedOffset(-1)     // Not yet known
                .triggerTopic(trigger != null ? trigger.getTopic() : null)
                .triggerPartition(trigger != null ? trigger.getPartition() : -1)
                .triggerOffset(trigger != null ? trigger.getOffset() : -1)
                .triggerConsumerGroup(trigger != null ? trigger.getGroup() : null)
                .producerTimestampMs(System.currentTimeMillis())
                .clientId(config.getClientId())
                .applicationName(config.getApplicationName())
                .messageKey(null)       // Per spec: do NOT access record key
                .messageSizeBytes(0)    // Per spec: do NOT estimate size
                .schemaVersion(1)
                .build();
    }
}
