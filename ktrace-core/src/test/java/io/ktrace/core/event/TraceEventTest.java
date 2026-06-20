package io.ktrace.core.event;

import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for TraceEvent - Scenario 1: Root Event (No Trigger)
 * Based on specs/trace-event-schema.spec.md
 */
class TraceEventTest {

    @Test
    void rootEvent_shouldHaveNullParentAndTrigger() {
        // Given: A root event with no trigger context
        TraceEvent event = TraceEvent.builder()
                .traceId(UUID.randomUUID().toString())
                .spanId(UUID.randomUUID().toString())
                .parentSpanId(null)
                .producedTopic("orders")
                .producedPartition(-1)
                .producedOffset(-1)
                .triggerTopic(null)
                .triggerPartition(-1)
                .triggerOffset(-1)
                .triggerConsumerGroup(null)
                .producerTimestampMs(System.currentTimeMillis())
                .clientId("producer-1")
                .applicationName("order-service")
                .messageKey("order-123")
                .messageSizeBytes(256)
                .schemaVersion(1)
                .build();

        // Then: Verify root event characteristics
        assertThat(event.getParentSpanId()).isNull();
        assertThat(event.getTriggerTopic()).isNull();
        assertThat(event.getTriggerConsumerGroup()).isNull();
        assertThat(event.getProducedPartition()).isEqualTo(-1);
        assertThat(event.getProducedOffset()).isEqualTo(-1);
    }

    @Test
    void builder_shouldRequireTraceId() {
        // When: Building without traceId
        // Then: Should throw NPE
        assertThatThrownBy(() ->
                TraceEvent.builder()
                        .spanId(UUID.randomUUID().toString())
                        .producedTopic("orders")
                        .producedPartition(-1)
                        .producedOffset(-1)
                        .triggerPartition(-1)
                        .triggerOffset(-1)
                        .producerTimestampMs(System.currentTimeMillis())
                        .clientId("producer-1")
                        .messageSizeBytes(256)
                        .schemaVersion(1)
                        .build()
        ).isInstanceOf(NullPointerException.class)
                .hasMessageContaining("traceId");
    }

    @Test
    void builder_shouldRequireSpanId() {
        // When: Building without spanId
        // Then: Should throw NPE
        assertThatThrownBy(() ->
                TraceEvent.builder()
                        .traceId(UUID.randomUUID().toString())
                        .producedTopic("orders")
                        .producedPartition(-1)
                        .producedOffset(-1)
                        .triggerPartition(-1)
                        .triggerOffset(-1)
                        .producerTimestampMs(System.currentTimeMillis())
                        .clientId("producer-1")
                        .messageSizeBytes(256)
                        .schemaVersion(1)
                        .build()
        ).isInstanceOf(NullPointerException.class)
                .hasMessageContaining("spanId");
    }

    @Test
    void builder_shouldRequireProducedTopic() {
        // When: Building without producedTopic
        // Then: Should throw NPE
        assertThatThrownBy(() ->
                TraceEvent.builder()
                        .traceId(UUID.randomUUID().toString())
                        .spanId(UUID.randomUUID().toString())
                        .producedPartition(-1)
                        .producedOffset(-1)
                        .triggerPartition(-1)
                        .triggerOffset(-1)
                        .producerTimestampMs(System.currentTimeMillis())
                        .clientId("producer-1")
                        .messageSizeBytes(256)
                        .schemaVersion(1)
                        .build()
        ).isInstanceOf(NullPointerException.class)
                .hasMessageContaining("producedTopic");
    }

    @Test
    void builder_shouldValidateTraceIdUuidFormat() {
        // When: Building with invalid UUID format
        // Then: Should throw IllegalArgumentException
        assertThatThrownBy(() ->
                TraceEvent.builder()
                        .traceId("not-a-valid-uuid")
                        .spanId(UUID.randomUUID().toString())
                        .producedTopic("orders")
                        .producedPartition(-1)
                        .producedOffset(-1)
                        .triggerPartition(-1)
                        .triggerOffset(-1)
                        .producerTimestampMs(System.currentTimeMillis())
                        .clientId("producer-1")
                        .messageSizeBytes(256)
                        .schemaVersion(1)
                        .build()
        ).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("traceId must be a valid UUID");
    }

    @Test
    void builder_shouldValidateSpanIdUuidFormat() {
        // When: Building with invalid UUID format
        // Then: Should throw IllegalArgumentException
        assertThatThrownBy(() ->
                TraceEvent.builder()
                        .traceId(UUID.randomUUID().toString())
                        .spanId("not-a-valid-uuid")
                        .producedTopic("orders")
                        .producedPartition(-1)
                        .producedOffset(-1)
                        .triggerPartition(-1)
                        .triggerOffset(-1)
                        .producerTimestampMs(System.currentTimeMillis())
                        .clientId("producer-1")
                        .messageSizeBytes(256)
                        .schemaVersion(1)
                        .build()
        ).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("spanId must be a valid UUID");
    }

    @Test
    void builder_shouldAcceptValidUuid() {
        // Given: Valid UUID strings
        String traceId = "550e8400-e29b-41d4-a716-446655440000";
        String spanId = "6ba7b810-9dad-11d1-80b4-00c04fd430c8";

        // When: Building with valid UUIDs
        TraceEvent event = TraceEvent.builder()
                .traceId(traceId)
                .spanId(spanId)
                .producedTopic("orders")
                .producedPartition(-1)
                .producedOffset(-1)
                .triggerPartition(-1)
                .triggerOffset(-1)
                .producerTimestampMs(System.currentTimeMillis())
                .clientId("producer-1")
                .messageSizeBytes(256)
                .schemaVersion(1)
                .build();

        // Then: Should succeed
        assertThat(event.getTraceId()).isEqualTo(traceId);
        assertThat(event.getSpanId()).isEqualTo(spanId);
    }

    @Test
    void traceEvent_shouldBeImmutable() {
        // Given: A TraceEvent
        TraceEvent event = TraceEvent.builder()
                .traceId(UUID.randomUUID().toString())
                .spanId(UUID.randomUUID().toString())
                .producedTopic("orders")
                .producedPartition(-1)
                .producedOffset(-1)
                .triggerPartition(-1)
                .triggerOffset(-1)
                .producerTimestampMs(System.currentTimeMillis())
                .clientId("producer-1")
                .messageSizeBytes(256)
                .schemaVersion(1)
                .build();

        // Then: Should have no setters (verified at compile time)
        // This test documents the immutability requirement
        assertThat(event).isNotNull();
    }

    @Test
    void equals_shouldReturnTrueForSameData() {
        // Given: Two events with identical data
        String traceId = UUID.randomUUID().toString();
        String spanId = UUID.randomUUID().toString();
        long timestamp = System.currentTimeMillis();

        TraceEvent event1 = TraceEvent.builder()
                .traceId(traceId)
                .spanId(spanId)
                .producedTopic("orders")
                .producedPartition(-1)
                .producedOffset(-1)
                .triggerPartition(-1)
                .triggerOffset(-1)
                .producerTimestampMs(timestamp)
                .clientId("producer-1")
                .messageSizeBytes(256)
                .schemaVersion(1)
                .build();

        TraceEvent event2 = TraceEvent.builder()
                .traceId(traceId)
                .spanId(spanId)
                .producedTopic("orders")
                .producedPartition(-1)
                .producedOffset(-1)
                .triggerPartition(-1)
                .triggerOffset(-1)
                .producerTimestampMs(timestamp)
                .clientId("producer-1")
                .messageSizeBytes(256)
                .schemaVersion(1)
                .build();

        // Then: Should be equal
        assertThat(event1).isEqualTo(event2);
        assertThat(event1.hashCode()).isEqualTo(event2.hashCode());
    }

    @Test
    void equals_shouldReturnFalseForDifferentSpanId() {
        // Given: Two events with different spanId
        String traceId = UUID.randomUUID().toString();

        TraceEvent event1 = TraceEvent.builder()
                .traceId(traceId)
                .spanId(UUID.randomUUID().toString())
                .producedTopic("orders")
                .producedPartition(-1)
                .producedOffset(-1)
                .triggerPartition(-1)
                .triggerOffset(-1)
                .producerTimestampMs(System.currentTimeMillis())
                .clientId("producer-1")
                .messageSizeBytes(256)
                .schemaVersion(1)
                .build();

        TraceEvent event2 = TraceEvent.builder()
                .traceId(traceId)
                .spanId(UUID.randomUUID().toString())
                .producedTopic("orders")
                .producedPartition(-1)
                .producedOffset(-1)
                .triggerPartition(-1)
                .triggerOffset(-1)
                .producerTimestampMs(System.currentTimeMillis())
                .clientId("producer-1")
                .messageSizeBytes(256)
                .schemaVersion(1)
                .build();

        // Then: Should NOT be equal
        assertThat(event1).isNotEqualTo(event2);
    }

    @Test
    void messageKey_shouldAllowNull() {
        // Given: Building without messageKey (nullable field)
        TraceEvent event = TraceEvent.builder()
                .traceId(UUID.randomUUID().toString())
                .spanId(UUID.randomUUID().toString())
                .producedTopic("orders")
                .producedPartition(-1)
                .producedOffset(-1)
                .triggerPartition(-1)
                .triggerOffset(-1)
                .producerTimestampMs(System.currentTimeMillis())
                .clientId("producer-1")
                .messageSizeBytes(256)
                .schemaVersion(1)
                .build();

        // Then: messageKey should be null
        assertThat(event.getMessageKey()).isNull();
    }

    @Test
    void applicationName_shouldAllowNull() {
        // Given: Building without applicationName (nullable field)
        TraceEvent event = TraceEvent.builder()
                .traceId(UUID.randomUUID().toString())
                .spanId(UUID.randomUUID().toString())
                .producedTopic("orders")
                .producedPartition(-1)
                .producedOffset(-1)
                .triggerPartition(-1)
                .triggerOffset(-1)
                .producerTimestampMs(System.currentTimeMillis())
                .clientId("producer-1")
                .messageSizeBytes(256)
                .schemaVersion(1)
                .build();

        // Then: applicationName should be null
        assertThat(event.getApplicationName()).isNull();
    }

    // Scenario 2: Child Event (With Trigger)

    @Test
    void childEvent_shouldLinkToParentViaParentSpanId() {
        // Given: parent event context
        String parentTraceId = "550e8400-e29b-41d4-a716-446655440000";
        String parentSpanId = "6ba7b810-9dad-11d1-80b4-00c04fd430c8";

        // When: child event is created
        String childSpanId = "7c9e6679-7425-40de-944b-e07fc1f90ae7";
        TraceEvent childEvent = TraceEvent.builder()
                .traceId(parentTraceId)
                .spanId(childSpanId)
                .parentSpanId(parentSpanId)  // ← Links to parent
                .producedTopic("notifications")
                .producedPartition(-1)
                .producedOffset(-1)
                .triggerTopic("orders")
                .triggerPartition(2)
                .triggerOffset(100L)
                .triggerConsumerGroup("order-processor-group")
                .producerTimestampMs(1718582401000L)
                .clientId("producer-1")
                .applicationName("notification-service")
                .messageKey("notification-456")
                .messageSizeBytes(128)
                .schemaVersion(1)
                .build();

        // Then: child should link to parent
        assertThat(childEvent.getTraceId()).isEqualTo(parentTraceId);
        assertThat(childEvent.getSpanId()).isEqualTo(childSpanId);
        assertThat(childEvent.getParentSpanId()).isEqualTo(parentSpanId);
        assertThat(childEvent.getSpanId()).isNotEqualTo(parentSpanId);  // New span!
    }

    @Test
    void childEvent_shouldPreserveTraceId() {
        // Given: parent trace ID
        String parentTraceId = "550e8400-e29b-41d4-a716-446655440000";
        String parentSpanId = "6ba7b810-9dad-11d1-80b4-00c04fd430c8";

        // When: child event is created
        TraceEvent childEvent = TraceEvent.builder()
                .traceId(parentTraceId)  // ← Same trace ID (chain preserved)
                .spanId("7c9e6679-7425-40de-944b-e07fc1f90ae7")
                .parentSpanId(parentSpanId)
                .producedTopic("notifications")
                .producedPartition(-1)
                .producedOffset(-1)
                .triggerTopic("orders")
                .triggerPartition(2)
                .triggerOffset(100L)
                .triggerConsumerGroup("order-processor-group")
                .producerTimestampMs(System.currentTimeMillis())
                .clientId("producer-1")
                .messageSizeBytes(128)
                .schemaVersion(1)
                .build();

        // Then: trace ID should be preserved
        assertThat(childEvent.getTraceId()).isEqualTo(parentTraceId);
    }

    @Test
    void childEvent_shouldHaveTriggerMetadata() {
        // When: child event with trigger is created
        TraceEvent childEvent = TraceEvent.builder()
                .traceId("550e8400-e29b-41d4-a716-446655440000")
                .spanId("7c9e6679-7425-40de-944b-e07fc1f90ae7")
                .parentSpanId("6ba7b810-9dad-11d1-80b4-00c04fd430c8")
                .producedTopic("notifications")
                .producedPartition(-1)
                .producedOffset(-1)
                .triggerTopic("orders")          // ← Trigger metadata
                .triggerPartition(2)
                .triggerOffset(100L)
                .triggerConsumerGroup("order-processor-group")
                .producerTimestampMs(System.currentTimeMillis())
                .clientId("producer-1")
                .messageSizeBytes(128)
                .schemaVersion(1)
                .build();

        // Then: trigger metadata should be set
        assertThat(childEvent.getTriggerTopic()).isEqualTo("orders");
        assertThat(childEvent.getTriggerPartition()).isEqualTo(2);
        assertThat(childEvent.getTriggerOffset()).isEqualTo(100L);
        assertThat(childEvent.getTriggerConsumerGroup()).isEqualTo("order-processor-group");
    }

    @Test
    void childEvent_shouldAllowNullTriggerConsumerGroup() {
        // When: child event with null consumer group
        TraceEvent childEvent = TraceEvent.builder()
                .traceId("550e8400-e29b-41d4-a716-446655440000")
                .spanId("7c9e6679-7425-40de-944b-e07fc1f90ae7")
                .parentSpanId("6ba7b810-9dad-11d1-80b4-00c04fd430c8")
                .producedTopic("notifications")
                .producedPartition(-1)
                .producedOffset(-1)
                .triggerTopic("orders")
                .triggerPartition(2)
                .triggerOffset(100L)
                .triggerConsumerGroup(null)  // ← Nullable
                .producerTimestampMs(System.currentTimeMillis())
                .clientId("producer-1")
                .messageSizeBytes(128)
                .schemaVersion(1)
                .build();

        // Then: trigger consumer group should be null
        assertThat(childEvent.getTriggerConsumerGroup()).isNull();
    }
}
