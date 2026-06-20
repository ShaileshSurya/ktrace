package io.ktrace.core.context;

import java.util.Objects;
import java.util.UUID;

/**
 * Immutable trace context for causality tracking across Kafka messages.
 * <p>
 * A TraceContext represents a point in the causal chain:
 * - {@code traceId} identifies the top-level causal chain
 * - {@code spanId} identifies this specific produce/consume operation
 * - {@code parentSpanId} links to the triggering operation (null for root spans)
 * <p>
 * Context is stored in ThreadLocal to enable zero-config propagation through
 * Kafka producer interceptors. Applications can use {@link #current()} to read
 * the active context and {@link #setCurrent(TraceContext)} to set it.
 * <p>
 * Thread safety: Each thread has its own isolated context via ThreadLocal.
 * Contexts do not leak across threads unless explicitly passed.
 *
 * @see io.ktrace.core.context.KTraceMDC for MDC integration
 * @see io.ktrace.core.context.TraceContextPropagator for Kafka header propagation
 */
public final class TraceContext {

    private static final ThreadLocal<TraceContext> CURRENT = new ThreadLocal<>();
    private static final ThreadLocal<TriggerMetadata> TRIGGER = new ThreadLocal<>();

    private final String traceId;
    private final String spanId;
    private final String parentSpanId;

    /**
     * Creates a new TraceContext.
     *
     * @param traceId      the trace UUID (must be valid UUID v4)
     * @param spanId       the span UUID (must be valid UUID v4)
     * @param parentSpanId the parent span UUID (nullable for root spans)
     * @throws NullPointerException     if traceId or spanId is null
     * @throws IllegalArgumentException if traceId or spanId is not a valid UUID
     */
    public TraceContext(String traceId, String spanId, String parentSpanId) {
        this.traceId = requireNonNull(traceId, "traceId");
        this.spanId = requireNonNull(spanId, "spanId");
        this.parentSpanId = parentSpanId;

        validateUuid(this.traceId, "traceId");
        validateUuid(this.spanId, "spanId");
        if (this.parentSpanId != null) {
            validateUuid(this.parentSpanId, "parentSpanId");
        }
    }

    /**
     * Creates a root TraceContext (no parent).
     *
     * @param traceId the trace UUID
     * @param spanId  the span UUID
     * @return a new root context
     */
    public static TraceContext root(String traceId, String spanId) {
        return new TraceContext(traceId, spanId, null);
    }

    /**
     * Creates a child TraceContext that links to a parent.
     *
     * @param traceId      the trace UUID (inherited from parent)
     * @param spanId       the new span UUID (unique for this operation)
     * @param parentSpanId the parent span UUID (links to triggering operation)
     * @return a new child context
     */
    public static TraceContext child(String traceId, String spanId, String parentSpanId) {
        return new TraceContext(traceId, spanId, parentSpanId);
    }

    /**
     * Gets the current TraceContext from ThreadLocal storage.
     *
     * @return the current context, or null if none set
     */
    public static TraceContext current() {
        return CURRENT.get();
    }

    /**
     * Gets the current TraceContext, or creates a root context if none exists.
     * <p>
     * This method ensures every thread has a trace context:
     * - If context already exists in ThreadLocal: return it
     * - If no context: create new root (no parent, new traceId and spanId)
     * <p>
     * Useful for producers that generate new chains or consumers receiving
     * messages without ktrace headers.
     *
     * @return current context if present, or newly created root context
     */
    public static TraceContext currentOrCreateRoot() {
        TraceContext current = CURRENT.get();
        if (current != null) {
            return current;
        }

        String traceId = UUID.randomUUID().toString();
        String spanId = UUID.randomUUID().toString();
        TraceContext root = new TraceContext(traceId, spanId, null);

        CURRENT.set(root);
        return root;
    }

    /**
     * Sets the current TraceContext in ThreadLocal storage.
     * <p>
     * This should be called by consumers after extracting context from Kafka headers.
     * The producer interceptor will read this context when producing messages.
     * <p>
     * IMPORTANT: Always call {@link #clearCurrent()} after processing to prevent
     * ThreadLocal memory leaks.
     *
     * @param context the context to set (null to clear)
     */
    public static void setCurrent(TraceContext context) {
        if (context == null) {
            CURRENT.remove();
        } else {
            CURRENT.set(context);
        }
    }

    /**
     * Clears the current TraceContext from ThreadLocal storage.
     * <p>
     * This must be called in a finally block or onAcknowledgement callback to
     * prevent ThreadLocal memory leaks.
     */
    public static void clearCurrent() {
        CURRENT.remove();
        TRIGGER.remove();
    }

    /**
     * Sets trigger metadata for consumer-to-producer flows.
     * <p>
     * This should be called after extracting context from a consumed message,
     * storing the topic/partition/offset/group that triggered this operation.
     *
     * @param topic     the trigger topic
     * @param partition the trigger partition
     * @param offset    the trigger offset
     * @param group     the consumer group (nullable)
     */
    public static void setTrigger(String topic, int partition, long offset, String group) {
        TRIGGER.set(new TriggerMetadata(topic, partition, offset, group));
    }

    /**
     * Gets the trigger metadata set via {@link #setTrigger}.
     *
     * @return the trigger metadata, or null if none set
     */
    public static TriggerMetadata getTrigger() {
        return TRIGGER.get();
    }

    private static String requireNonNull(String value, String fieldName) {
        if (value == null) {
            throw new NullPointerException(fieldName + " must not be null");
        }
        return value;
    }

    private static void validateUuid(String value, String fieldName) {
        try {
            UUID.fromString(value);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(fieldName + " must be a valid UUID: " + value, e);
        }
    }

    public String getTraceId() {
        return traceId;
    }

    public String getSpanId() {
        return spanId;
    }

    public String getParentSpanId() {
        return parentSpanId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TraceContext that = (TraceContext) o;
        return Objects.equals(traceId, that.traceId) &&
                Objects.equals(spanId, that.spanId) &&
                Objects.equals(parentSpanId, that.parentSpanId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(traceId, spanId, parentSpanId);
    }

    @Override
    public String toString() {
        return "TraceContext{" +
                "traceId='" + traceId + '\'' +
                ", spanId='" + spanId + '\'' +
                ", parentSpanId='" + parentSpanId + '\'' +
                '}';
    }

    /**
     * Trigger metadata for consumer-to-producer flows.
     */
    public static final class TriggerMetadata {
        private final String topic;
        private final int partition;
        private final long offset;
        private final String group;

        public TriggerMetadata(String topic, int partition, long offset, String group) {
            this.topic = topic;
            this.partition = partition;
            this.offset = offset;
            this.group = group;
        }

        public String getTopic() {
            return topic;
        }

        public int getPartition() {
            return partition;
        }

        public long getOffset() {
            return offset;
        }

        public String getGroup() {
            return group;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            TriggerMetadata that = (TriggerMetadata) o;
            return partition == that.partition &&
                    offset == that.offset &&
                    Objects.equals(topic, that.topic) &&
                    Objects.equals(group, that.group);
        }

        @Override
        public int hashCode() {
            return Objects.hash(topic, partition, offset, group);
        }

        @Override
        public String toString() {
            return "TriggerMetadata{" +
                    "topic='" + topic + '\'' +
                    ", partition=" + partition +
                    ", offset=" + offset +
                    ", group='" + group + '\'' +
                    '}';
        }
    }
}
