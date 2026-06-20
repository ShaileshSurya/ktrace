package io.ktrace.core.event;

import org.apache.kafka.common.errors.SerializationException;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for TraceEvent JSON serialization round-trip - Scenario 4
 * Based on specs/trace-event-schema.spec.md
 * <p>
 * Validates that TraceEvent objects survive Kafka transport without data corruption.
 * This ensures producer-side serialization and tracer-side deserialization preserve
 * all fields exactly, which is critical for causality chain reconstruction.
 */
class TraceEventSerializationTest {

    private final TraceEventSerializer serializer = new TraceEventSerializer();
    private final TraceEventDeserializer deserializer = new TraceEventDeserializer();

    @Test
    void roundTrip_allFieldsPopulated_shouldPreserveAllData() {
        // Given: Fully populated TraceEvent
        TraceEvent original = TraceEvent.builder()
                .traceId("550e8400-e29b-41d4-a716-446655440000")
                .spanId("6ba7b810-9dad-11d1-80b4-00c04fd430c8")
                .parentSpanId("7c9e6679-7425-40de-944b-e07fc1f90ae7")
                .producedTopic("orders")
                .producedPartition(2)
                .producedOffset(12345L)
                .triggerTopic("payments")
                .triggerPartition(1)
                .triggerOffset(67890L)
                .triggerConsumerGroup("order-processor")
                .producerTimestampMs(1718841234567L)
                .clientId("producer-1")
                .applicationName("order-service")
                .messageKey("order-123")
                .messageSizeBytes(1024)
                .schemaVersion(1)
                .build();

        // When: Serialize and deserialize
        byte[] json = serializer.serialize("__ktrace", original);
        TraceEvent deserialized = deserializer.deserialize("__ktrace", json);

        // Then: All fields must match
        assertThat(deserialized).isEqualTo(original);
        assertThat(deserialized.hashCode()).isEqualTo(original.hashCode());
    }

    @Test
    void roundTrip_nullableFieldsNull_shouldPreserveNulls() {
        // Given: Root event with nulls
        TraceEvent original = TraceEvent.builder()
                .traceId("550e8400-e29b-41d4-a716-446655440000")
                .spanId("6ba7b810-9dad-11d1-80b4-00c04fd430c8")
                .parentSpanId(null)  // ← Root span
                .producedTopic("orders")
                .producedPartition(-1)
                .producedOffset(-1)
                .triggerTopic(null)  // ← No trigger
                .triggerPartition(-1)
                .triggerOffset(-1)
                .triggerConsumerGroup(null)
                .producerTimestampMs(1718841234567L)
                .clientId("producer-1")
                .applicationName(null)  // ← Optional
                .messageKey(null)  // ← No key
                .messageSizeBytes(256)
                .schemaVersion(1)
                .build();

        // When: Round-trip
        byte[] json = serializer.serialize("__ktrace", original);
        TraceEvent deserialized = deserializer.deserialize("__ktrace", json);

        // Then: Nulls remain null (NOT empty strings or 0)
        assertThat(deserialized.getParentSpanId()).isNull();
        assertThat(deserialized.getTriggerTopic()).isNull();
        assertThat(deserialized.getTriggerConsumerGroup()).isNull();
        assertThat(deserialized.getApplicationName()).isNull();
        assertThat(deserialized.getMessageKey()).isNull();

        // Primitives should keep their sentinel values
        assertThat(deserialized.getProducedPartition()).isEqualTo(-1);
        assertThat(deserialized.getProducedOffset()).isEqualTo(-1);
    }

    @Test
    void roundTrip_timestampPrecision_shouldPreserveMilliseconds() {
        // Given: Event with precise timestamp
        long preciseTimestamp = 1718841234567L;  // 13 digits
        TraceEvent original = TraceEvent.builder()
                .traceId("550e8400-e29b-41d4-a716-446655440000")
                .spanId("6ba7b810-9dad-11d1-80b4-00c04fd430c8")
                .producedTopic("orders")
                .producedPartition(-1)
                .producedOffset(-1)
                .triggerPartition(-1)
                .triggerOffset(-1)
                .producerTimestampMs(preciseTimestamp)
                .clientId("producer-1")
                .messageSizeBytes(256)
                .schemaVersion(1)
                .build();

        // When: Round-trip
        byte[] json = serializer.serialize("__ktrace", original);
        TraceEvent deserialized = deserializer.deserialize("__ktrace", json);

        // Then: No precision loss
        assertThat(deserialized.getProducerTimestampMs()).isEqualTo(preciseTimestamp);
    }

    @Test
    void roundTrip_largeOffsetValues_shouldPreserveLongRange() {
        // Given: Large offset values (long range)
        long largeOffset = 9_223_372_036_854_775_807L;  // Long.MAX_VALUE
        TraceEvent original = TraceEvent.builder()
                .traceId("550e8400-e29b-41d4-a716-446655440000")
                .spanId("6ba7b810-9dad-11d1-80b4-00c04fd430c8")
                .producedTopic("orders")
                .producedPartition(0)
                .producedOffset(largeOffset)
                .triggerPartition(-1)
                .triggerOffset(-1)
                .producerTimestampMs(System.currentTimeMillis())
                .clientId("producer-1")
                .messageSizeBytes(256)
                .schemaVersion(1)
                .build();

        // When: Round-trip
        byte[] json = serializer.serialize("__ktrace", original);
        TraceEvent deserialized = deserializer.deserialize("__ktrace", json);

        // Then: Long values preserved
        assertThat(deserialized.getProducedOffset()).isEqualTo(largeOffset);
    }

    @Test
    void serialize_nullEvent_shouldReturnNull() {
        // When: Serialize null
        byte[] result = serializer.serialize("__ktrace", null);

        // Then: Should return null (not throw)
        assertThat(result).isNull();
    }

    @Test
    void deserialize_nullBytes_shouldReturnNull() {
        // When: Deserialize null
        TraceEvent result = deserializer.deserialize("__ktrace", null);

        // Then: Should return null (not throw)
        assertThat(result).isNull();
    }

    @Test
    void deserialize_emptyBytes_shouldThrowException() {
        // When/Then: Empty bytes should fail
        assertThatThrownBy(() ->
                deserializer.deserialize("__ktrace", new byte[0])
        ).isInstanceOf(SerializationException.class);
    }

    @Test
    void deserialize_invalidJson_shouldThrowException() {
        // Given: Invalid JSON
        byte[] invalidJson = "not-valid-json".getBytes();

        // When/Then: Should throw
        assertThatThrownBy(() ->
                deserializer.deserialize("__ktrace", invalidJson)
        ).isInstanceOf(SerializationException.class)
                .hasMessageContaining("Failed to deserialize");
    }

    @Test
    void roundTrip_specialCharacters_shouldPreserveUnicode() {
        // Given: Event with unicode/special chars
        TraceEvent original = TraceEvent.builder()
                .traceId("550e8400-e29b-41d4-a716-446655440000")
                .spanId("6ba7b810-9dad-11d1-80b4-00c04fd430c8")
                .producedTopic("orders")
                .producedPartition(-1)
                .producedOffset(-1)
                .triggerPartition(-1)
                .triggerOffset(-1)
                .producerTimestampMs(System.currentTimeMillis())
                .clientId("producer-1")
                .applicationName("测试-service-🚀")  // Chinese + emoji
                .messageKey("key-with-\"quotes\"-and-\nnewlines")
                .messageSizeBytes(256)
                .schemaVersion(1)
                .build();

        // When: Round-trip
        byte[] json = serializer.serialize("__ktrace", original);
        TraceEvent deserialized = deserializer.deserialize("__ktrace", json);

        // Then: Special chars preserved
        assertThat(deserialized.getApplicationName()).isEqualTo("测试-service-🚀");
        assertThat(deserialized.getMessageKey()).isEqualTo("key-with-\"quotes\"-and-\nnewlines");
    }
}
