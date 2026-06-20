package io.ktrace.core.event;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

/**
 * Manual inspection test to verify JSON structure.
 * Not part of automated test suite - for manual verification only.
 */
class JsonInspectionTest {

    private final TraceEventSerializer serializer = new TraceEventSerializer();

    @Test
    void inspectJson_fullEvent() {
        TraceEvent event = TraceEvent.builder()
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

        byte[] json = serializer.serialize("__ktrace", event);
        System.out.println("=== Full Event JSON ===");
        System.out.println(new String(json, StandardCharsets.UTF_8));
    }

    @Test
    void inspectJson_rootEvent() {
        TraceEvent event = TraceEvent.builder()
                .traceId("550e8400-e29b-41d4-a716-446655440000")
                .spanId("6ba7b810-9dad-11d1-80b4-00c04fd430c8")
                .parentSpanId(null)
                .producedTopic("orders")
                .producedPartition(-1)
                .producedOffset(-1)
                .triggerTopic(null)
                .triggerPartition(-1)
                .triggerOffset(-1)
                .triggerConsumerGroup(null)
                .producerTimestampMs(1718841234567L)
                .clientId("producer-1")
                .applicationName(null)
                .messageKey(null)
                .messageSizeBytes(256)
                .schemaVersion(1)
                .build();

        byte[] json = serializer.serialize("__ktrace", event);
        System.out.println("=== Root Event JSON (with nulls) ===");
        System.out.println(new String(json, StandardCharsets.UTF_8));
    }
}
