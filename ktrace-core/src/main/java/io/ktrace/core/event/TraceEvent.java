package io.ktrace.core.event;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;
import java.util.UUID;

/**
 * Immutable representation of a Kafka produce event for causality tracing.
 * <p>
 * A TraceEvent is published to the __ktrace topic whenever a producer sends a message.
 * It contains the trace context (traceId, spanId, parentSpanId) and metadata about
 * the produced message.
 * <p>
 * This class is immutable - all fields are final and there are no setters.
 *
 * @see <a href="specs/trace-event-schema.spec.md">TraceEvent Schema Spec</a>
 */
public final class TraceEvent {

    // Trace context
    @JsonProperty("traceId")
    private final String traceId;

    @JsonProperty("spanId")
    private final String spanId;

    @JsonProperty("parentSpanId")
    private final String parentSpanId;

    // Produced message metadata
    @JsonProperty("producedTopic")
    private final String producedTopic;

    @JsonProperty("producedPartition")
    private final int producedPartition;

    @JsonProperty("producedOffset")
    private final long producedOffset;

    // Trigger metadata (what caused this produce)
    @JsonProperty("triggerTopic")
    private final String triggerTopic;

    @JsonProperty("triggerPartition")
    private final int triggerPartition;

    @JsonProperty("triggerOffset")
    private final long triggerOffset;

    @JsonProperty("triggerConsumerGroup")
    private final String triggerConsumerGroup;

    // Producer metadata
    @JsonProperty("producerTimestampMs")
    private final long producerTimestampMs;

    @JsonProperty("clientId")
    private final String clientId;

    @JsonProperty("applicationName")
    private final String applicationName;

    // Message metadata
    @JsonProperty("messageKey")
    private final String messageKey;

    @JsonProperty("messageSizeBytes")
    private final int messageSizeBytes;

    // Schema versioning
    @JsonProperty("schemaVersion")
    private final int schemaVersion;

    @JsonCreator
    private TraceEvent(
            @JsonProperty("traceId") String traceId,
            @JsonProperty("spanId") String spanId,
            @JsonProperty("parentSpanId") String parentSpanId,
            @JsonProperty("producedTopic") String producedTopic,
            @JsonProperty("producedPartition") int producedPartition,
            @JsonProperty("producedOffset") long producedOffset,
            @JsonProperty("triggerTopic") String triggerTopic,
            @JsonProperty("triggerPartition") int triggerPartition,
            @JsonProperty("triggerOffset") long triggerOffset,
            @JsonProperty("triggerConsumerGroup") String triggerConsumerGroup,
            @JsonProperty("producerTimestampMs") long producerTimestampMs,
            @JsonProperty("clientId") String clientId,
            @JsonProperty("applicationName") String applicationName,
            @JsonProperty("messageKey") String messageKey,
            @JsonProperty("messageSizeBytes") int messageSizeBytes,
            @JsonProperty("schemaVersion") int schemaVersion) {

        // Validate required fields
        this.traceId = requireNonNull(traceId, "traceId");
        this.spanId = requireNonNull(spanId, "spanId");
        this.producedTopic = requireNonNull(producedTopic, "producedTopic");
        this.clientId = requireNonNull(clientId, "clientId");

        // Validate UUID format
        validateUuid(this.traceId, "traceId");
        validateUuid(this.spanId, "spanId");
        if (parentSpanId != null) {
            validateUuid(parentSpanId, "parentSpanId");
        }

        // Nullable fields
        this.parentSpanId = parentSpanId;
        this.triggerTopic = triggerTopic;
        this.triggerConsumerGroup = triggerConsumerGroup;
        this.applicationName = applicationName;
        this.messageKey = messageKey;

        // Primitive fields
        this.producedPartition = producedPartition;
        this.producedOffset = producedOffset;
        this.triggerPartition = triggerPartition;
        this.triggerOffset = triggerOffset;
        this.producerTimestampMs = producerTimestampMs;
        this.messageSizeBytes = messageSizeBytes;
        this.schemaVersion = schemaVersion;
    }

    // Builder constructor delegates to main constructor
    private TraceEvent(Builder builder) {
        this(builder.traceId, builder.spanId, builder.parentSpanId,
             builder.producedTopic, builder.producedPartition, builder.producedOffset,
             builder.triggerTopic, builder.triggerPartition, builder.triggerOffset,
             builder.triggerConsumerGroup, builder.producerTimestampMs, builder.clientId,
             builder.applicationName, builder.messageKey, builder.messageSizeBytes,
             builder.schemaVersion);
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

    // Getters

    public String getTraceId() {
        return traceId;
    }

    public String getSpanId() {
        return spanId;
    }

    public String getParentSpanId() {
        return parentSpanId;
    }

    public String getProducedTopic() {
        return producedTopic;
    }

    public int getProducedPartition() {
        return producedPartition;
    }

    public long getProducedOffset() {
        return producedOffset;
    }

    public String getTriggerTopic() {
        return triggerTopic;
    }

    public int getTriggerPartition() {
        return triggerPartition;
    }

    public long getTriggerOffset() {
        return triggerOffset;
    }

    public String getTriggerConsumerGroup() {
        return triggerConsumerGroup;
    }

    public long getProducerTimestampMs() {
        return producerTimestampMs;
    }

    public String getClientId() {
        return clientId;
    }

    public String getApplicationName() {
        return applicationName;
    }

    public String getMessageKey() {
        return messageKey;
    }

    public int getMessageSizeBytes() {
        return messageSizeBytes;
    }

    public int getSchemaVersion() {
        return schemaVersion;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        TraceEvent that = (TraceEvent) o;
        return producedPartition == that.producedPartition &&
                producedOffset == that.producedOffset &&
                triggerPartition == that.triggerPartition &&
                triggerOffset == that.triggerOffset &&
                producerTimestampMs == that.producerTimestampMs &&
                messageSizeBytes == that.messageSizeBytes &&
                schemaVersion == that.schemaVersion &&
                Objects.equals(traceId, that.traceId) &&
                Objects.equals(spanId, that.spanId) &&
                Objects.equals(parentSpanId, that.parentSpanId) &&
                Objects.equals(producedTopic, that.producedTopic) &&
                Objects.equals(triggerTopic, that.triggerTopic) &&
                Objects.equals(triggerConsumerGroup, that.triggerConsumerGroup) &&
                Objects.equals(clientId, that.clientId) &&
                Objects.equals(applicationName, that.applicationName) &&
                Objects.equals(messageKey, that.messageKey);
    }

    @Override
    public int hashCode() {
        return Objects.hash(traceId, spanId, parentSpanId, producedTopic, producedPartition,
                producedOffset, triggerTopic, triggerPartition, triggerOffset, triggerConsumerGroup,
                producerTimestampMs, clientId, applicationName, messageKey, messageSizeBytes,
                schemaVersion);
    }

    @Override
    public String toString() {
        return "TraceEvent{" +
                "traceId='" + traceId + '\'' +
                ", spanId='" + spanId + '\'' +
                ", parentSpanId='" + parentSpanId + '\'' +
                ", producedTopic='" + producedTopic + '\'' +
                ", producedPartition=" + producedPartition +
                ", producedOffset=" + producedOffset +
                ", triggerTopic='" + triggerTopic + '\'' +
                ", triggerPartition=" + triggerPartition +
                ", triggerOffset=" + triggerOffset +
                ", triggerConsumerGroup='" + triggerConsumerGroup + '\'' +
                ", producerTimestampMs=" + producerTimestampMs +
                ", clientId='" + clientId + '\'' +
                ", applicationName='" + applicationName + '\'' +
                ", messageKey='" + messageKey + '\'' +
                ", messageSizeBytes=" + messageSizeBytes +
                ", schemaVersion=" + schemaVersion +
                '}';
    }

    /**
     * Creates a new Builder for constructing TraceEvent instances.
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Builder for TraceEvent following the fluent API pattern.
     */
    public static final class Builder {
        private String traceId;
        private String spanId;
        private String parentSpanId;
        private String producedTopic;
        private int producedPartition;
        private long producedOffset;
        private String triggerTopic;
        private int triggerPartition;
        private long triggerOffset;
        private String triggerConsumerGroup;
        private long producerTimestampMs;
        private String clientId;
        private String applicationName;
        private String messageKey;
        private int messageSizeBytes;
        private int schemaVersion;

        private Builder() {
        }

        public Builder traceId(String traceId) {
            this.traceId = traceId;
            return this;
        }

        public Builder spanId(String spanId) {
            this.spanId = spanId;
            return this;
        }

        public Builder parentSpanId(String parentSpanId) {
            this.parentSpanId = parentSpanId;
            return this;
        }

        public Builder producedTopic(String producedTopic) {
            this.producedTopic = producedTopic;
            return this;
        }

        public Builder producedPartition(int producedPartition) {
            this.producedPartition = producedPartition;
            return this;
        }

        public Builder producedOffset(long producedOffset) {
            this.producedOffset = producedOffset;
            return this;
        }

        public Builder triggerTopic(String triggerTopic) {
            this.triggerTopic = triggerTopic;
            return this;
        }

        public Builder triggerPartition(int triggerPartition) {
            this.triggerPartition = triggerPartition;
            return this;
        }

        public Builder triggerOffset(long triggerOffset) {
            this.triggerOffset = triggerOffset;
            return this;
        }

        public Builder triggerConsumerGroup(String triggerConsumerGroup) {
            this.triggerConsumerGroup = triggerConsumerGroup;
            return this;
        }

        public Builder producerTimestampMs(long producerTimestampMs) {
            this.producerTimestampMs = producerTimestampMs;
            return this;
        }

        public Builder clientId(String clientId) {
            this.clientId = clientId;
            return this;
        }

        public Builder applicationName(String applicationName) {
            this.applicationName = applicationName;
            return this;
        }

        public Builder messageKey(String messageKey) {
            this.messageKey = messageKey;
            return this;
        }

        public Builder messageSizeBytes(int messageSizeBytes) {
            this.messageSizeBytes = messageSizeBytes;
            return this;
        }

        public Builder schemaVersion(int schemaVersion) {
            this.schemaVersion = schemaVersion;
            return this;
        }

        /**
         * Builds the TraceEvent instance.
         * Validates that all required fields are set and in the correct format.
         *
         * @return a new immutable TraceEvent instance
         * @throws NullPointerException if any required field is null
         * @throws IllegalArgumentException if UUID fields are not valid UUIDs
         */
        public TraceEvent build() {
            return new TraceEvent(this);
        }
    }
}
