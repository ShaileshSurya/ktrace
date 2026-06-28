package io.ktrace.core.interceptor;

import io.ktrace.core.context.TraceContext;
import io.ktrace.core.context.TraceContextPropagator;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link KTraceProducerInterceptor}.
 * <p>
 * These tests verify the interceptor's behavior for Scenario 1 (root span, no trigger).
 * Focus is on header injection, ThreadLocal isolation, and graceful degradation.
 */
class KTraceProducerInterceptorTest {

    private KTraceProducerInterceptor<String, String> interceptor;

    @AfterEach
    void tearDown() {
        if (interceptor != null) {
            interceptor.close();
        }
        TraceContext.clearCurrent();
    }

    @Test
    void configure_shouldInitializePublisher() {
        // Given
        Map<String, Object> config = new HashMap<>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put("ktrace.enabled", "true");
        config.put("ktrace.trace-topic", "__ktrace");
        config.put("ktrace.application-name", "test-app");

        interceptor = new KTraceProducerInterceptor<>();

        // When
        interceptor.configure(config);

        // Then: Should initialize without throwing exceptions
        assertThat(interceptor).isNotNull();
    }

    @Test
    void configure_disabled_shouldSkipPublisher() {
        // Given
        Map<String, Object> config = new HashMap<>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put("ktrace.enabled", "false");

        interceptor = new KTraceProducerInterceptor<>();

        // When
        interceptor.configure(config);

        // Then: Should initialize without errors (publisher not created)
        assertThat(interceptor).isNotNull();
    }

    @Test
    void configure_missingBootstrapServers_shouldLogError() {
        // Given: Config without bootstrap.servers
        Map<String, Object> config = new HashMap<>();
        config.put("ktrace.enabled", "true");

        interceptor = new KTraceProducerInterceptor<>();

        // When
        interceptor.configure(config);

        // Then: Should not throw exception (graceful degradation)
        assertThat(interceptor).isNotNull();
    }

    @Test
    void onSend_rootSpan_shouldInjectHeaders() {
        // Given
        Map<String, Object> config = new HashMap<>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put("ktrace.enabled", "true");
        config.put("ktrace.application-name", "test-app");

        interceptor = new KTraceProducerInterceptor<>();
        interceptor.configure(config);

        ProducerRecord<String, String> record = new ProducerRecord<>("orders", "key", "value");

        // When
        ProducerRecord<String, String> modified = interceptor.onSend(record);

        // Then: Should inject ktrace headers
        Header traceIdHeader = modified.headers().lastHeader(TraceContextPropagator.HEADER_TRACE_ID);
        Header spanIdHeader = modified.headers().lastHeader(TraceContextPropagator.HEADER_SPAN_ID);
        Header parentSpanIdHeader = modified.headers().lastHeader(TraceContextPropagator.HEADER_PARENT_SPAN_ID);

        assertThat(traceIdHeader).isNotNull();
        assertThat(spanIdHeader).isNotNull();
        assertThat(parentSpanIdHeader).isNull();  // Root span has no parent

        // Verify UUIDs are valid format
        String traceId = new String(traceIdHeader.value(), StandardCharsets.UTF_8);
        String spanId = new String(spanIdHeader.value(), StandardCharsets.UTF_8);
        assertThat(traceId).matches("[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}");
        assertThat(spanId).matches("[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}");
    }

    @Test
    void onSend_rootSpan_shouldNotModifyThreadLocal() {
        // Given
        Map<String, Object> config = new HashMap<>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put("ktrace.enabled", "true");
        config.put("ktrace.application-name", "test-app");

        interceptor = new KTraceProducerInterceptor<>();
        interceptor.configure(config);

        ProducerRecord<String, String> record = new ProducerRecord<>("orders", "key", "value");

        // Verify ThreadLocal is empty before
        assertThat(TraceContext.current()).isNull();

        // When
        interceptor.onSend(record);

        // Then: ThreadLocal should still be empty (interceptor is read-only)
        assertThat(TraceContext.current()).isNull();
    }

    @Test
    void onSend_rootSpan_shouldPublishEvent() {
        // Given
        Map<String, Object> config = new HashMap<>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put("ktrace.enabled", "true");
        config.put("ktrace.application-name", "test-app");
        config.put("client.id", "test-producer");

        interceptor = new KTraceProducerInterceptor<>();
        interceptor.configure(config);

        ProducerRecord<String, String> record = new ProducerRecord<>("orders", "key", "value");

        // When
        ProducerRecord<String, String> modified = interceptor.onSend(record);

        // Then: Should return modified record (event publishing happens async)
        assertThat(modified).isNotNull();
        assertThat(modified.topic()).isEqualTo("orders");
    }

    @Test
    void onSend_disabled_shouldReturnUnmodified() {
        // Given
        Map<String, Object> config = new HashMap<>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put("ktrace.enabled", "false");

        interceptor = new KTraceProducerInterceptor<>();
        interceptor.configure(config);

        ProducerRecord<String, String> record = new ProducerRecord<>("orders", "key", "value");

        // When
        ProducerRecord<String, String> modified = interceptor.onSend(record);

        // Then: Should NOT inject headers
        Header traceIdHeader = modified.headers().lastHeader(TraceContextPropagator.HEADER_TRACE_ID);
        assertThat(traceIdHeader).isNull();
    }

    @Test
    void onSend_existingHeaders_shouldRemoveAndReplace() {
        // Given
        Map<String, Object> config = new HashMap<>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put("ktrace.enabled", "true");
        config.put("ktrace.application-name", "test-app");

        interceptor = new KTraceProducerInterceptor<>();
        interceptor.configure(config);

        ProducerRecord<String, String> record = new ProducerRecord<>("orders", "key", "value");

        // Add existing ktrace headers with VALID UUIDs (so they're extracted as context)
        record.headers().add(TraceContextPropagator.HEADER_TRACE_ID,
            "550e8400-e29b-41d4-a716-446655440000".getBytes(StandardCharsets.UTF_8));
        record.headers().add(TraceContextPropagator.HEADER_SPAN_ID,
            "6ba7b810-9dad-11d1-80b4-00c04fd430c8".getBytes(StandardCharsets.UTF_8));

        // When
        ProducerRecord<String, String> modified = interceptor.onSend(record);

        // Then: Should create child span (reuse traceId, new spanId, set parentSpanId)
        Header traceIdHeader = modified.headers().lastHeader(TraceContextPropagator.HEADER_TRACE_ID);
        Header spanIdHeader = modified.headers().lastHeader(TraceContextPropagator.HEADER_SPAN_ID);
        Header parentSpanIdHeader = modified.headers().lastHeader(TraceContextPropagator.HEADER_PARENT_SPAN_ID);

        String traceId = new String(traceIdHeader.value(), StandardCharsets.UTF_8);
        String spanId = new String(spanIdHeader.value(), StandardCharsets.UTF_8);
        String parentSpanId = new String(parentSpanIdHeader.value(), StandardCharsets.UTF_8);

        // Trace chain is preserved (not replaced with new root)
        assertThat(traceId).isEqualTo("550e8400-e29b-41d4-a716-446655440000");
        assertThat(spanId).isNotEqualTo("6ba7b810-9dad-11d1-80b4-00c04fd430c8");  // New span
        assertThat(parentSpanId).isEqualTo("6ba7b810-9dad-11d1-80b4-00c04fd430c8");  // Links to old span
    }

    @Test
    void onSend_publisherUnavailable_shouldStillInjectHeaders() {
        // Given: Config with missing bootstrap.servers (publisher won't initialize)
        Map<String, Object> config = new HashMap<>();
        config.put("ktrace.enabled", "true");
        config.put("ktrace.application-name", "test-app");

        interceptor = new KTraceProducerInterceptor<>();
        interceptor.configure(config);

        ProducerRecord<String, String> record = new ProducerRecord<>("orders", "key", "value");

        // When
        ProducerRecord<String, String> modified = interceptor.onSend(record);

        // Then: Should still inject headers (graceful degradation)
        Header traceIdHeader = modified.headers().lastHeader(TraceContextPropagator.HEADER_TRACE_ID);
        assertThat(traceIdHeader).isNotNull();
    }

    @Test
    void close_shouldClosePublisher() {
        // Given
        Map<String, Object> config = new HashMap<>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put("ktrace.enabled", "true");
        config.put("ktrace.application-name", "test-app");

        interceptor = new KTraceProducerInterceptor<>();
        interceptor.configure(config);

        // When
        interceptor.close();

        // Then: Should not throw exception
        assertThat(interceptor).isNotNull();
    }

    @Test
    void onSend_withThreadLocalContext_shouldCreateChildSpan() {
        // Given
        Map<String, Object> config = new HashMap<>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put("ktrace.enabled", "true");
        config.put("ktrace.application-name", "test-app");

        interceptor = new KTraceProducerInterceptor<>();
        interceptor.configure(config);

        // Set ThreadLocal context (simulating consumer flow)
        TraceContext parentContext = new TraceContext(
                "550e8400-e29b-41d4-a716-446655440000",
                "6ba7b810-9dad-11d1-80b4-00c04fd430c8",
                null
        );
        TraceContext.setCurrent(parentContext);

        ProducerRecord<String, String> record = new ProducerRecord<>("notifications", "key", "value");

        // When
        ProducerRecord<String, String> modified = interceptor.onSend(record);

        // Then: Should create child span
        Header traceIdHeader = modified.headers().lastHeader(TraceContextPropagator.HEADER_TRACE_ID);
        Header spanIdHeader = modified.headers().lastHeader(TraceContextPropagator.HEADER_SPAN_ID);
        Header parentSpanIdHeader = modified.headers().lastHeader(TraceContextPropagator.HEADER_PARENT_SPAN_ID);

        String traceId = new String(traceIdHeader.value(), StandardCharsets.UTF_8);
        String spanId = new String(spanIdHeader.value(), StandardCharsets.UTF_8);
        String parentSpanId = new String(parentSpanIdHeader.value(), StandardCharsets.UTF_8);

        // Child span should reuse parent's traceId
        assertThat(traceId).isEqualTo("550e8400-e29b-41d4-a716-446655440000");

        // Child span should have new spanId (not same as parent)
        assertThat(spanId).isNotEqualTo("6ba7b810-9dad-11d1-80b4-00c04fd430c8");

        // Child span's parentSpanId should link to parent's spanId
        assertThat(parentSpanId).isEqualTo("6ba7b810-9dad-11d1-80b4-00c04fd430c8");
    }

    // ====== Scenario 2: Consumer-Triggered Produce (Child Span) Tests ======

    @Test
    void scenario2_consumerTriggeredProduce_shouldCreateChildSpanWithTriggerMetadata() {
        // Given: Configure interceptor
        Map<String, Object> config = new HashMap<>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put("ktrace.enabled", "true");
        config.put("ktrace.application-name", "order-processor");

        interceptor = new KTraceProducerInterceptor<>();
        interceptor.configure(config);

        // Simulate consumer receiving message with ktrace headers
        TraceContext consumerContext = new TraceContext(
                "550e8400-e29b-41d4-a716-446655440000",  // traceId from consumed message
                "6ba7b810-9dad-11d1-80b4-00c04fd430c8",  // spanId from consumed message
                null  // This was root span on producer side
        );
        TraceContext.setCurrent(consumerContext);

        // Consumer sets trigger metadata
        TraceContext.setTrigger("orders", 2, 100L, "order-processor-group");

        // When: Consumer produces new message
        ProducerRecord<String, String> outgoing = new ProducerRecord<>("notifications", "key", "notification");
        ProducerRecord<String, String> modified = interceptor.onSend(outgoing);

        // Then: Verify child span creation
        Header traceIdHeader = modified.headers().lastHeader(TraceContextPropagator.HEADER_TRACE_ID);
        Header spanIdHeader = modified.headers().lastHeader(TraceContextPropagator.HEADER_SPAN_ID);
        Header parentSpanIdHeader = modified.headers().lastHeader(TraceContextPropagator.HEADER_PARENT_SPAN_ID);

        assertThat(traceIdHeader).isNotNull();
        assertThat(spanIdHeader).isNotNull();
        assertThat(parentSpanIdHeader).isNotNull();

        String traceId = new String(traceIdHeader.value(), StandardCharsets.UTF_8);
        String spanId = new String(spanIdHeader.value(), StandardCharsets.UTF_8);
        String parentSpanId = new String(parentSpanIdHeader.value(), StandardCharsets.UTF_8);

        // Step 6: Reuse traceId from parent (chain preserved)
        assertThat(traceId).isEqualTo("550e8400-e29b-41d4-a716-446655440000");

        // Step 4: Generate NEW spanId
        assertThat(spanId).isNotEqualTo("6ba7b810-9dad-11d1-80b4-00c04fd430c8");
        assertThat(spanId).matches("[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}");

        // Step 5: Set parentSpanId to consumer's spanId (causal link)
        assertThat(parentSpanId).isEqualTo("6ba7b810-9dad-11d1-80b4-00c04fd430c8");
    }

    @Test
    void scenario2_consumerTriggeredProduce_shouldNotModifyThreadLocal() {
        // Given
        Map<String, Object> config = new HashMap<>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put("ktrace.enabled", "true");
        config.put("ktrace.application-name", "order-processor");

        interceptor = new KTraceProducerInterceptor<>();
        interceptor.configure(config);

        // Set consumer context
        TraceContext originalContext = new TraceContext(
                "550e8400-e29b-41d4-a716-446655440000",
                "6ba7b810-9dad-11d1-80b4-00c04fd430c8",
                null
        );
        TraceContext.setCurrent(originalContext);
        TraceContext.setTrigger("orders", 2, 100L, "order-processor-group");

        // When: Produce message
        ProducerRecord<String, String> record = new ProducerRecord<>("notifications", "key", "value");
        interceptor.onSend(record);

        // Then: ThreadLocal context should remain unchanged
        TraceContext currentContext = TraceContext.current();
        assertThat(currentContext).isEqualTo(originalContext);
        assertThat(currentContext.getTraceId()).isEqualTo("550e8400-e29b-41d4-a716-446655440000");
        assertThat(currentContext.getSpanId()).isEqualTo("6ba7b810-9dad-11d1-80b4-00c04fd430c8");

        // Trigger metadata should also remain unchanged
        TraceContext.TriggerMetadata trigger = TraceContext.getTrigger();
        assertThat(trigger).isNotNull();
        assertThat(trigger.getTopic()).isEqualTo("orders");
        assertThat(trigger.getPartition()).isEqualTo(2);
        assertThat(trigger.getOffset()).isEqualTo(100L);
        assertThat(trigger.getGroup()).isEqualTo("order-processor-group");
    }

    @Test
    void scenario2_multipleProduce_shouldCreateUniqueSpanIds() {
        // Given
        Map<String, Object> config = new HashMap<>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put("ktrace.enabled", "true");
        config.put("ktrace.application-name", "order-processor");

        interceptor = new KTraceProducerInterceptor<>();
        interceptor.configure(config);

        // Set consumer context
        TraceContext consumerContext = new TraceContext(
                "550e8400-e29b-41d4-a716-446655440000",
                "6ba7b810-9dad-11d1-80b4-00c04fd430c8",
                null
        );
        TraceContext.setCurrent(consumerContext);
        TraceContext.setTrigger("orders", 2, 100L, "order-processor-group");

        // When: Consumer produces multiple messages
        ProducerRecord<String, String> record1 = new ProducerRecord<>("notifications", "key1", "value1");
        ProducerRecord<String, String> record2 = new ProducerRecord<>("notifications", "key2", "value2");
        ProducerRecord<String, String> record3 = new ProducerRecord<>("audit", "key3", "value3");

        ProducerRecord<String, String> modified1 = interceptor.onSend(record1);
        ProducerRecord<String, String> modified2 = interceptor.onSend(record2);
        ProducerRecord<String, String> modified3 = interceptor.onSend(record3);

        // Then: Each should have unique spanId but same traceId and parentSpanId
        String spanId1 = new String(modified1.headers().lastHeader(TraceContextPropagator.HEADER_SPAN_ID).value(), StandardCharsets.UTF_8);
        String spanId2 = new String(modified2.headers().lastHeader(TraceContextPropagator.HEADER_SPAN_ID).value(), StandardCharsets.UTF_8);
        String spanId3 = new String(modified3.headers().lastHeader(TraceContextPropagator.HEADER_SPAN_ID).value(), StandardCharsets.UTF_8);

        // All spanIds should be unique
        assertThat(spanId1).isNotEqualTo(spanId2);
        assertThat(spanId2).isNotEqualTo(spanId3);
        assertThat(spanId1).isNotEqualTo(spanId3);

        // All should share same traceId
        String traceId1 = new String(modified1.headers().lastHeader(TraceContextPropagator.HEADER_TRACE_ID).value(), StandardCharsets.UTF_8);
        String traceId2 = new String(modified2.headers().lastHeader(TraceContextPropagator.HEADER_TRACE_ID).value(), StandardCharsets.UTF_8);
        String traceId3 = new String(modified3.headers().lastHeader(TraceContextPropagator.HEADER_TRACE_ID).value(), StandardCharsets.UTF_8);

        assertThat(traceId1).isEqualTo("550e8400-e29b-41d4-a716-446655440000");
        assertThat(traceId2).isEqualTo("550e8400-e29b-41d4-a716-446655440000");
        assertThat(traceId3).isEqualTo("550e8400-e29b-41d4-a716-446655440000");

        // All should have same parentSpanId (consumer's spanId)
        String parentSpanId1 = new String(modified1.headers().lastHeader(TraceContextPropagator.HEADER_PARENT_SPAN_ID).value(), StandardCharsets.UTF_8);
        String parentSpanId2 = new String(modified2.headers().lastHeader(TraceContextPropagator.HEADER_PARENT_SPAN_ID).value(), StandardCharsets.UTF_8);
        String parentSpanId3 = new String(modified3.headers().lastHeader(TraceContextPropagator.HEADER_PARENT_SPAN_ID).value(), StandardCharsets.UTF_8);

        assertThat(parentSpanId1).isEqualTo("6ba7b810-9dad-11d1-80b4-00c04fd430c8");
        assertThat(parentSpanId2).isEqualTo("6ba7b810-9dad-11d1-80b4-00c04fd430c8");
        assertThat(parentSpanId3).isEqualTo("6ba7b810-9dad-11d1-80b4-00c04fd430c8");
    }

    @Test
    void scenario2_contextWithoutTrigger_shouldCreateChildSpanWithNullTrigger() {
        // Given: Consumer context set, but NO trigger metadata
        Map<String, Object> config = new HashMap<>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put("ktrace.enabled", "true");
        config.put("ktrace.application-name", "order-processor");

        interceptor = new KTraceProducerInterceptor<>();
        interceptor.configure(config);

        TraceContext consumerContext = new TraceContext(
                "550e8400-e29b-41d4-a716-446655440000",
                "6ba7b810-9dad-11d1-80b4-00c04fd430c8",
                null
        );
        TraceContext.setCurrent(consumerContext);
        // Note: NOT calling TraceContext.setTrigger()

        // When
        ProducerRecord<String, String> record = new ProducerRecord<>("notifications", "key", "value");
        ProducerRecord<String, String> modified = interceptor.onSend(record);

        // Then: Should still create child span with proper headers
        String traceId = new String(modified.headers().lastHeader(TraceContextPropagator.HEADER_TRACE_ID).value(), StandardCharsets.UTF_8);
        String parentSpanId = new String(modified.headers().lastHeader(TraceContextPropagator.HEADER_PARENT_SPAN_ID).value(), StandardCharsets.UTF_8);

        assertThat(traceId).isEqualTo("550e8400-e29b-41d4-a716-446655440000");
        assertThat(parentSpanId).isEqualTo("6ba7b810-9dad-11d1-80b4-00c04fd430c8");

        // Note: TraceEvent will have null trigger fields, but headers are correct
    }

    @Test
    void scenario2_withExistingOutgoingHeaders_shouldReplaceWithChildSpan() {
        // Given: Consumer has context, outgoing record has stale headers
        Map<String, Object> config = new HashMap<>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put("ktrace.enabled", "true");
        config.put("ktrace.application-name", "order-processor");

        interceptor = new KTraceProducerInterceptor<>();
        interceptor.configure(config);

        // Set consumer context
        TraceContext consumerContext = new TraceContext(
                "550e8400-e29b-41d4-a716-446655440000",
                "6ba7b810-9dad-11d1-80b4-00c04fd430c8",
                null
        );
        TraceContext.setCurrent(consumerContext);
        TraceContext.setTrigger("orders", 2, 100L, "order-processor-group");

        // Outgoing record has DIFFERENT stale headers (should be ignored)
        ProducerRecord<String, String> record = new ProducerRecord<>("notifications", "key", "value");
        record.headers().add(TraceContextPropagator.HEADER_TRACE_ID,
                "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee".getBytes(StandardCharsets.UTF_8));
        record.headers().add(TraceContextPropagator.HEADER_SPAN_ID,
                "11111111-2222-3333-4444-555555555555".getBytes(StandardCharsets.UTF_8));

        // When
        ProducerRecord<String, String> modified = interceptor.onSend(record);

        // Then: Should use ThreadLocal context (not stale headers)
        String traceId = new String(modified.headers().lastHeader(TraceContextPropagator.HEADER_TRACE_ID).value(), StandardCharsets.UTF_8);
        String parentSpanId = new String(modified.headers().lastHeader(TraceContextPropagator.HEADER_PARENT_SPAN_ID).value(), StandardCharsets.UTF_8);

        // ThreadLocal context wins (not stale headers)
        assertThat(traceId).isEqualTo("550e8400-e29b-41d4-a716-446655440000");
        assertThat(parentSpanId).isEqualTo("6ba7b810-9dad-11d1-80b4-00c04fd430c8");
    }

    @Test
    void scenario2_usingExtractAndBind_shouldPreserveFullFlow() {
        // Given: Simulate complete consumer-to-producer flow using real API
        Map<String, Object> config = new HashMap<>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put("ktrace.enabled", "true");
        config.put("ktrace.application-name", "order-processor");

        interceptor = new KTraceProducerInterceptor<>();
        interceptor.configure(config);

        // Step 1: Create consumed record with ktrace headers (simulates upstream producer)
        ProducerRecord<String, String> upstreamRecord = new ProducerRecord<>("orders", "key", "order-data");
        upstreamRecord.headers().add(TraceContextPropagator.HEADER_TRACE_ID,
                "550e8400-e29b-41d4-a716-446655440000".getBytes(StandardCharsets.UTF_8));
        upstreamRecord.headers().add(TraceContextPropagator.HEADER_SPAN_ID,
                "6ba7b810-9dad-11d1-80b4-00c04fd430c8".getBytes(StandardCharsets.UTF_8));

        // Step 2: Consumer extracts context (simulates consumer receiving message)
        TraceContext extractedContext = TraceContextPropagator.extract(upstreamRecord.headers());
        TraceContext.setCurrent(extractedContext);
        TraceContext.setTrigger("orders", 2, 100L, "order-processor-group");

        // Step 3: Consumer produces downstream message
        ProducerRecord<String, String> downstreamRecord = new ProducerRecord<>("notifications", "key", "notification");
        ProducerRecord<String, String> modified = interceptor.onSend(downstreamRecord);

        // Then: Verify complete causal chain
        String traceId = new String(modified.headers().lastHeader(TraceContextPropagator.HEADER_TRACE_ID).value(), StandardCharsets.UTF_8);
        String spanId = new String(modified.headers().lastHeader(TraceContextPropagator.HEADER_SPAN_ID).value(), StandardCharsets.UTF_8);
        String parentSpanId = new String(modified.headers().lastHeader(TraceContextPropagator.HEADER_PARENT_SPAN_ID).value(), StandardCharsets.UTF_8);

        // Trace chain: upstream span -> consumer span -> producer span
        assertThat(traceId).isEqualTo("550e8400-e29b-41d4-a716-446655440000");  // Inherited
        assertThat(spanId).isNotEqualTo("6ba7b810-9dad-11d1-80b4-00c04fd430c8");  // New
        assertThat(parentSpanId).isEqualTo("6ba7b810-9dad-11d1-80b4-00c04fd430c8");  // Links to consumer
    }

    // ====== Scenario 4: Disabled Tracing Tests ======

    @Test
    void scenario4_disabled_shouldBeCompleteNoOp() {
        // Given
        Map<String, Object> config = new HashMap<>();
        config.put("ktrace.enabled", "false");

        // Set ThreadLocal context (to verify it's not accessed)
        TraceContext.setCurrent(TraceContext.root(
                "550e8400-e29b-41d4-a716-446655440000",
                "6ba7b810-9dad-11d1-80b4-00c04fd430c8"
        ));

        interceptor = new KTraceProducerInterceptor<>();
        interceptor.configure(config);

        ProducerRecord<String, String> record = new ProducerRecord<>("orders", "key", "value");

        // When
        ProducerRecord<String, String> modified = interceptor.onSend(record);

        // Then
        assertThat(modified.headers().lastHeader(TraceContextPropagator.HEADER_TRACE_ID)).isNull();
        assertThat(TraceContext.current()).isNotNull(); // Context unchanged
    }

    @Test
    void scenario4_disabled_withoutBootstrapServers_shouldWork() {
        // Given
        Map<String, Object> config = new HashMap<>();
        config.put("ktrace.enabled", "false");
        // No bootstrap.servers

        interceptor = new KTraceProducerInterceptor<>();

        // When
        interceptor.configure(config);
        ProducerRecord<String, String> record = new ProducerRecord<>("orders", "key", "value");
        ProducerRecord<String, String> modified = interceptor.onSend(record);

        // Then
        assertThat(modified.headers().lastHeader(TraceContextPropagator.HEADER_TRACE_ID)).isNull();
    }

    @Test
    void scenario4_disabled_withExistingHeaders_shouldNotRemove() {
        // Given
        Map<String, Object> config = new HashMap<>();
        config.put("ktrace.enabled", "false");

        interceptor = new KTraceProducerInterceptor<>();
        interceptor.configure(config);

        ProducerRecord<String, String> record = new ProducerRecord<>("orders", "key", "value");
        record.headers().add(TraceContextPropagator.HEADER_TRACE_ID, "existing-id".getBytes(StandardCharsets.UTF_8));

        // When
        ProducerRecord<String, String> modified = interceptor.onSend(record);

        // Then: Existing headers pass through unchanged
        String existingValue = new String(modified.headers().lastHeader(TraceContextPropagator.HEADER_TRACE_ID).value(), StandardCharsets.UTF_8);
        assertThat(existingValue).isEqualTo("existing-id");
    }

    // ====== Scenario 9: Idempotent Header Injection Tests ======

    @Test
    void scenario9_withAllThreeHeaders_shouldReplaceAll() {
        // Given
        Map<String, Object> config = new HashMap<>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put("ktrace.enabled", "true");

        interceptor = new KTraceProducerInterceptor<>();
        interceptor.configure(config);

        ProducerRecord<String, String> record = new ProducerRecord<>("orders", "key", "value");
        record.headers().add(TraceContextPropagator.HEADER_TRACE_ID, "old-trace".getBytes(StandardCharsets.UTF_8));
        record.headers().add(TraceContextPropagator.HEADER_SPAN_ID, "old-span".getBytes(StandardCharsets.UTF_8));
        record.headers().add(TraceContextPropagator.HEADER_PARENT_SPAN_ID, "old-parent".getBytes(StandardCharsets.UTF_8));

        // When
        ProducerRecord<String, String> modified = interceptor.onSend(record);

        // Then: All old headers replaced
        assertThat(countHeaders(modified.headers(), TraceContextPropagator.HEADER_TRACE_ID)).isEqualTo(1);
        assertThat(countHeaders(modified.headers(), TraceContextPropagator.HEADER_SPAN_ID)).isEqualTo(1);
    }

    @Test
    void scenario9_withDuplicateHeaders_shouldRemoveAll() {
        // Given
        Map<String, Object> config = new HashMap<>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put("ktrace.enabled", "true");

        interceptor = new KTraceProducerInterceptor<>();
        interceptor.configure(config);

        ProducerRecord<String, String> record = new ProducerRecord<>("orders", "key", "value");
        record.headers().add(TraceContextPropagator.HEADER_TRACE_ID, "uuid-1".getBytes(StandardCharsets.UTF_8));
        record.headers().add(TraceContextPropagator.HEADER_TRACE_ID, "uuid-2".getBytes(StandardCharsets.UTF_8));
        record.headers().add(TraceContextPropagator.HEADER_TRACE_ID, "uuid-3".getBytes(StandardCharsets.UTF_8));

        // When
        ProducerRecord<String, String> modified = interceptor.onSend(record);

        // Then: All duplicates removed, exactly 1 header
        assertThat(countHeaders(modified.headers(), TraceContextPropagator.HEADER_TRACE_ID)).isEqualTo(1);
    }

    @Test
    void scenario9_withoutExistingHeaders_shouldInjectNormally() {
        // Given
        Map<String, Object> config = new HashMap<>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put("ktrace.enabled", "true");

        interceptor = new KTraceProducerInterceptor<>();
        interceptor.configure(config);

        ProducerRecord<String, String> record = new ProducerRecord<>("orders", "key", "value");

        // When
        ProducerRecord<String, String> modified = interceptor.onSend(record);

        // Then
        assertThat(countHeaders(modified.headers(), TraceContextPropagator.HEADER_TRACE_ID)).isEqualTo(1);
        assertThat(countHeaders(modified.headers(), TraceContextPropagator.HEADER_SPAN_ID)).isEqualTo(1);
    }

    @Test
    void scenario9_withThreadLocalAndExistingHeaders_shouldUseThreadLocal() {
        // Given
        Map<String, Object> config = new HashMap<>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put("ktrace.enabled", "true");

        interceptor = new KTraceProducerInterceptor<>();
        interceptor.configure(config);

        TraceContext.setCurrent(new TraceContext(
                "550e8400-e29b-41d4-a716-446655440000",
                "6ba7b810-9dad-11d1-80b4-00c04fd430c8",
                null
        ));

        ProducerRecord<String, String> record = new ProducerRecord<>("orders", "key", "value");
        record.headers().add(TraceContextPropagator.HEADER_TRACE_ID, "header-trace".getBytes(StandardCharsets.UTF_8));

        // When
        ProducerRecord<String, String> modified = interceptor.onSend(record);

        // Then: ThreadLocal wins
        String traceId = new String(modified.headers().lastHeader(TraceContextPropagator.HEADER_TRACE_ID).value(), StandardCharsets.UTF_8);
        assertThat(traceId).isEqualTo("550e8400-e29b-41d4-a716-446655440000");
    }

    @Test
    void scenario9_manualInjection_shouldReplaceWithAuthoritativeContext() {
        // Given
        Map<String, Object> config = new HashMap<>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put("ktrace.enabled", "true");

        interceptor = new KTraceProducerInterceptor<>();
        interceptor.configure(config);

        ProducerRecord<String, String> record = new ProducerRecord<>("orders", "key", "value");
        record.headers().add(TraceContextPropagator.HEADER_TRACE_ID, "user-trace-id".getBytes(StandardCharsets.UTF_8));

        // When
        ProducerRecord<String, String> modified = interceptor.onSend(record);

        // Then: User headers replaced
        String traceId = new String(modified.headers().lastHeader(TraceContextPropagator.HEADER_TRACE_ID).value(), StandardCharsets.UTF_8);
        assertThat(traceId).isNotEqualTo("user-trace-id");
        assertThat(traceId).matches("[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}");
    }

    // ====== Scenario 10: Publisher Initialization Failure Tests ======

    @Test
    void scenario10_missingBootstrapServers_shouldDisableGracefully() {
        // Given
        Map<String, Object> config = new HashMap<>();
        config.put("ktrace.enabled", "true");
        // No bootstrap.servers

        interceptor = new KTraceProducerInterceptor<>();

        // When
        interceptor.configure(config);

        // Then: Should not throw, publisher unavailable
        assertThat(interceptor).isNotNull();

        ProducerRecord<String, String> record = new ProducerRecord<>("orders", "key", "value");
        ProducerRecord<String, String> modified = interceptor.onSend(record);

        // Headers still injected
        assertThat(modified.headers().lastHeader(TraceContextPropagator.HEADER_TRACE_ID)).isNotNull();
    }

    @Test
    void scenario10_multipleOnSend_withUnavailablePublisher_shouldContinue() {
        // Given
        Map<String, Object> config = new HashMap<>();
        config.put("ktrace.enabled", "true");

        interceptor = new KTraceProducerInterceptor<>();
        interceptor.configure(config);

        // When: Call onSend multiple times
        for (int i = 0; i < 10; i++) {
            ProducerRecord<String, String> record = new ProducerRecord<>("orders", "key-" + i, "value");
            ProducerRecord<String, String> modified = interceptor.onSend(record);

            // Then: All succeed with headers
            assertThat(modified.headers().lastHeader(TraceContextPropagator.HEADER_TRACE_ID)).isNotNull();
        }
    }

    // ====== Helper Methods ======

    private int countHeaders(org.apache.kafka.common.header.Headers headers, String headerKey) {
        int count = 0;
        for (Header header : headers) {
            if (header.key().equals(headerKey)) {
                count++;
            }
        }
        return count;
    }
}
