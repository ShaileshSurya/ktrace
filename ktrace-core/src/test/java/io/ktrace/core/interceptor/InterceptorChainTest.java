package io.ktrace.core.interceptor;

import io.ktrace.core.context.TraceContext;
import io.ktrace.core.context.TraceContextPropagator;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.Header;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for Scenario 3: Interceptor Chain (Multiple Interceptors).
 * <p>
 * These tests verify that KTraceProducerInterceptor works correctly when
 * configured alongside other interceptors in a chain:
 * <ul>
 *   <li>Interceptors are called in configured order</li>
 *   <li>Later interceptors see headers injected by earlier ones</li>
 *   <li>ktrace does not interfere with other interceptors</li>
 *   <li>Header injection is idempotent</li>
 * </ul>
 */
class InterceptorChainTest {

    private KTraceProducerInterceptor<String, String> ktraceInterceptor;
    private CustomTestInterceptor customInterceptor;

    @BeforeEach
    void setUp() {
        Map<String, Object> config = new HashMap<>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put("ktrace.enabled", "true");
        config.put("ktrace.application-name", "test-app");
        config.put("client.id", "test-producer");

        ktraceInterceptor = new KTraceProducerInterceptor<>();
        ktraceInterceptor.configure(config);

        customInterceptor = new CustomTestInterceptor();
        customInterceptor.configure(config);
    }

    @AfterEach
    void tearDown() {
        if (ktraceInterceptor != null) {
            ktraceInterceptor.close();
        }
        if (customInterceptor != null) {
            customInterceptor.close();
        }
        TraceContext.clearCurrent();
    }

    @Test
    void ktraceRunsFirst_customInterceptorSeesHeaders() {
        // Given: Original record without headers
        ProducerRecord<String, String> record = new ProducerRecord<>("orders", "key", "value");

        // When: ktrace interceptor runs first
        ProducerRecord<String, String> afterKtrace = ktraceInterceptor.onSend(record);

        // Then: ktrace headers should be present
        assertThat(afterKtrace.headers().lastHeader(TraceContextPropagator.HEADER_TRACE_ID)).isNotNull();
        assertThat(afterKtrace.headers().lastHeader(TraceContextPropagator.HEADER_SPAN_ID)).isNotNull();

        // When: custom interceptor runs second
        ProducerRecord<String, String> afterCustom = customInterceptor.onSend(afterKtrace);

        // Then: custom interceptor should have seen ktrace headers
        ProducerRecord<String, String> capturedByCustom = customInterceptor.getLastRecordReceived();
        assertThat(capturedByCustom.headers().lastHeader(TraceContextPropagator.HEADER_TRACE_ID)).isNotNull();
        assertThat(capturedByCustom.headers().lastHeader(TraceContextPropagator.HEADER_SPAN_ID)).isNotNull();

        // Verify header values are valid UUIDs
        String traceId = new String(
                capturedByCustom.headers().lastHeader(TraceContextPropagator.HEADER_TRACE_ID).value(),
                StandardCharsets.UTF_8
        );
        String spanId = new String(
                capturedByCustom.headers().lastHeader(TraceContextPropagator.HEADER_SPAN_ID).value(),
                StandardCharsets.UTF_8
        );

        assertThat(traceId).matches("[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}");
        assertThat(spanId).matches("[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}");
    }

    @Test
    void multipleInterceptors_shouldNotInterfere() {
        // Given: Original record
        ProducerRecord<String, String> record = new ProducerRecord<>("orders", "key", "value");

        // When: Both interceptors run in sequence
        ProducerRecord<String, String> afterKtrace = ktraceInterceptor.onSend(record);
        customInterceptor.setCustomHeaderValue("custom-value-123");
        ProducerRecord<String, String> afterCustom = customInterceptor.onSend(afterKtrace);

        // Then: Both ktrace AND custom headers should be present
        Header ktraceTraceId = afterCustom.headers().lastHeader(TraceContextPropagator.HEADER_TRACE_ID);
        Header ktraceSpanId = afterCustom.headers().lastHeader(TraceContextPropagator.HEADER_SPAN_ID);
        Header customHeader = afterCustom.headers().lastHeader("custom-header");

        assertThat(ktraceTraceId).isNotNull();
        assertThat(ktraceSpanId).isNotNull();
        assertThat(customHeader).isNotNull();
        assertThat(new String(customHeader.value(), StandardCharsets.UTF_8)).isEqualTo("custom-value-123");

        // Verify ktrace headers remain unchanged by custom interceptor
        String traceId = new String(ktraceTraceId.value(), StandardCharsets.UTF_8);
        assertThat(traceId).matches("[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}");
    }

    @Test
    void idempotency_rerunningOnSendReplacesHeaders() {
        // Given: Original record
        ProducerRecord<String, String> record = new ProducerRecord<>("orders", "key", "value");

        // When: Run onSend twice
        ProducerRecord<String, String> firstRun = ktraceInterceptor.onSend(record);
        String firstTraceId = new String(
                firstRun.headers().lastHeader(TraceContextPropagator.HEADER_TRACE_ID).value(),
                StandardCharsets.UTF_8
        );
        String firstSpanId = new String(
                firstRun.headers().lastHeader(TraceContextPropagator.HEADER_SPAN_ID).value(),
                StandardCharsets.UTF_8
        );

        ProducerRecord<String, String> secondRun = ktraceInterceptor.onSend(firstRun);
        String secondTraceId = new String(
                secondRun.headers().lastHeader(TraceContextPropagator.HEADER_TRACE_ID).value(),
                StandardCharsets.UTF_8
        );
        String secondSpanId = new String(
                secondRun.headers().lastHeader(TraceContextPropagator.HEADER_SPAN_ID).value(),
                StandardCharsets.UTF_8
        );

        // Then: Headers should be replaced (different spanId, but forms child span)
        assertThat(secondTraceId).isEqualTo(firstTraceId);  // Same trace (child span)
        assertThat(secondSpanId).isNotEqualTo(firstSpanId);  // New span generated

        // Verify only ONE set of ktrace headers (no duplicates)
        int traceIdCount = 0;
        int spanIdCount = 0;
        for (Header header : secondRun.headers().toArray()) {
            if (header.key().equals(TraceContextPropagator.HEADER_TRACE_ID)) traceIdCount++;
            if (header.key().equals(TraceContextPropagator.HEADER_SPAN_ID)) spanIdCount++;
        }

        assertThat(traceIdCount).isEqualTo(1);
        assertThat(spanIdCount).isEqualTo(1);

        // Verify parent span link is established
        String parentSpanId = new String(
                secondRun.headers().lastHeader(TraceContextPropagator.HEADER_PARENT_SPAN_ID).value(),
                StandardCharsets.UTF_8
        );
        assertThat(parentSpanId).isEqualTo(firstSpanId);
    }

    @Test
    void customInterceptorModifiesRecord_ktraceHeadersRemainIntact() {
        // Given: Original record
        ProducerRecord<String, String> record = new ProducerRecord<>("orders", "key", "value");

        // When: ktrace runs first
        ProducerRecord<String, String> afterKtrace = ktraceInterceptor.onSend(record);
        String originalTraceId = new String(
                afterKtrace.headers().lastHeader(TraceContextPropagator.HEADER_TRACE_ID).value(),
                StandardCharsets.UTF_8
        );
        String originalSpanId = new String(
                afterKtrace.headers().lastHeader(TraceContextPropagator.HEADER_SPAN_ID).value(),
                StandardCharsets.UTF_8
        );

        // When: custom interceptor modifies topic, key, and value
        customInterceptor.setModifyRecord(true);
        ProducerRecord<String, String> afterCustom = customInterceptor.onSend(afterKtrace);

        // Then: Record should be modified
        assertThat(afterCustom.topic()).isEqualTo("modified-topic");
        assertThat(afterCustom.key()).isEqualTo("modified-key");
        assertThat(afterCustom.value()).isEqualTo("modified-value");

        // Then: ktrace headers should remain intact
        String finalTraceId = new String(
                afterCustom.headers().lastHeader(TraceContextPropagator.HEADER_TRACE_ID).value(),
                StandardCharsets.UTF_8
        );
        String finalSpanId = new String(
                afterCustom.headers().lastHeader(TraceContextPropagator.HEADER_SPAN_ID).value(),
                StandardCharsets.UTF_8
        );

        assertThat(finalTraceId).isEqualTo(originalTraceId);
        assertThat(finalSpanId).isEqualTo(originalSpanId);
    }

    @Test
    void customInterceptorThrowsException_ktraceStillWorks() {
        // Given: Original record
        ProducerRecord<String, String> record = new ProducerRecord<>("orders", "key", "value");

        // When: ktrace runs first (should succeed)
        ProducerRecord<String, String> afterKtrace = ktraceInterceptor.onSend(record);

        // Verify ktrace headers are injected
        assertThat(afterKtrace.headers().lastHeader(TraceContextPropagator.HEADER_TRACE_ID)).isNotNull();
        assertThat(afterKtrace.headers().lastHeader(TraceContextPropagator.HEADER_SPAN_ID)).isNotNull();

        // When: custom interceptor throws exception (simulating failure)
        customInterceptor.setThrowException(true);

        // Then: Custom interceptor fails, but ktrace already succeeded
        try {
            customInterceptor.onSend(afterKtrace);
        } catch (RuntimeException expected) {
            // Expected behavior - custom interceptor failed
        }

        // Verify ktrace headers are still present in the record
        assertThat(afterKtrace.headers().lastHeader(TraceContextPropagator.HEADER_TRACE_ID)).isNotNull();
        assertThat(afterKtrace.headers().lastHeader(TraceContextPropagator.HEADER_SPAN_ID)).isNotNull();
    }

    @Test
    void chainWithThreadLocalContext_customInterceptorSeesChildSpan() {
        // Given: Consumer context is set (simulating consumer-triggered produce)
        TraceContext consumerContext = new TraceContext(
                "550e8400-e29b-41d4-a716-446655440000",
                "6ba7b810-9dad-11d1-80b4-00c04fd430c8",
                null
        );
        TraceContext.setCurrent(consumerContext);
        TraceContext.setTrigger("orders", 2, 100L, "consumer-group");

        ProducerRecord<String, String> record = new ProducerRecord<>("notifications", "key", "value");

        // When: ktrace runs (should create child span)
        ProducerRecord<String, String> afterKtrace = ktraceInterceptor.onSend(record);

        // When: custom interceptor runs
        ProducerRecord<String, String> afterCustom = customInterceptor.onSend(afterKtrace);

        // Then: custom interceptor should see child span headers
        ProducerRecord<String, String> capturedByCustom = customInterceptor.getLastRecordReceived();
        String traceId = new String(
                capturedByCustom.headers().lastHeader(TraceContextPropagator.HEADER_TRACE_ID).value(),
                StandardCharsets.UTF_8
        );
        String parentSpanId = new String(
                capturedByCustom.headers().lastHeader(TraceContextPropagator.HEADER_PARENT_SPAN_ID).value(),
                StandardCharsets.UTF_8
        );

        // Verify child span created
        assertThat(traceId).isEqualTo("550e8400-e29b-41d4-a716-446655440000");  // Inherited
        assertThat(parentSpanId).isEqualTo("6ba7b810-9dad-11d1-80b4-00c04fd430c8");  // Links to consumer

        // Verify ThreadLocal context unchanged (ktrace is read-only)
        TraceContext afterContext = TraceContext.current();
        assertThat(afterContext).isEqualTo(consumerContext);
    }

    /**
     * Mock custom interceptor for testing interceptor chain behavior.
     * <p>
     * This interceptor can:
     * <ul>
     *   <li>Capture the record it receives (for inspection)</li>
     *   <li>Add custom headers</li>
     *   <li>Modify the record (topic, key, value)</li>
     *   <li>Throw exceptions (to test error handling)</li>
     * </ul>
     */
    static class CustomTestInterceptor implements ProducerInterceptor<String, String> {

        private ProducerRecord<String, String> lastRecordReceived;
        private String customHeaderValue;
        private boolean modifyRecord = false;
        private boolean throwException = false;

        @Override
        public ProducerRecord<String, String> onSend(ProducerRecord<String, String> record) {
            // Capture the record for inspection
            this.lastRecordReceived = record;

            // Simulate exception
            if (throwException) {
                throw new RuntimeException("Custom interceptor failed");
            }

            // Add custom header
            if (customHeaderValue != null) {
                record.headers().add("custom-header", customHeaderValue.getBytes(StandardCharsets.UTF_8));
            }

            // Optionally modify the record
            if (modifyRecord) {
                return new ProducerRecord<>(
                        "modified-topic",
                        record.partition(),
                        record.timestamp(),
                        "modified-key",
                        "modified-value",
                        record.headers()
                );
            }

            return record;
        }

        @Override
        public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
            // No-op
        }

        @Override
        public void close() {
            // No-op
        }

        @Override
        public void configure(Map<String, ?> configs) {
            // No-op
        }

        public ProducerRecord<String, String> getLastRecordReceived() {
            return lastRecordReceived;
        }

        public void setCustomHeaderValue(String value) {
            this.customHeaderValue = value;
        }

        public void setModifyRecord(boolean modify) {
            this.modifyRecord = modify;
        }

        public void setThrowException(boolean throwException) {
            this.throwException = throwException;
        }
    }
}
