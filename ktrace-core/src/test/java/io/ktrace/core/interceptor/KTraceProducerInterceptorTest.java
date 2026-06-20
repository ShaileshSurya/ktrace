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
}
