package io.ktrace.core.context;

import io.ktrace.core.config.KTraceConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.slf4j.MDC;

import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.*;

class TraceContextPropagatorTest {

    private final KTraceConfig originalConfig = KTraceConfig.getInstance();

    @AfterEach
    void cleanup() {
        KTraceMDC.clear();
        TraceContext.clearCurrent();
        KTraceConfig.setInstance(originalConfig);
    }

    @Test
    void extract_shouldReturnNullWhenHeadersNull() {
        TraceContext context = TraceContextPropagator.extract(null);

        assertThat(context).isNull();
    }

    @Test
    void extract_shouldReturnNullWhenTraceIdMissing() {
        Headers headers = new RecordHeaders();
        headers.add(TraceContextPropagator.HEADER_SPAN_ID, "span-123".getBytes(StandardCharsets.UTF_8));

        TraceContext context = TraceContextPropagator.extract(headers);

        assertThat(context).isNull();
    }

    @Test
    void extract_shouldReturnNullWhenSpanIdMissing() {
        Headers headers = new RecordHeaders();
        headers.add(TraceContextPropagator.HEADER_TRACE_ID, "trace-123".getBytes(StandardCharsets.UTF_8));

        TraceContext context = TraceContextPropagator.extract(headers);

        assertThat(context).isNull();
    }

    @Test
    void extract_shouldExtractRootContext() {
        String traceId = UUID.randomUUID().toString();
        String spanId = UUID.randomUUID().toString();

        Headers headers = new RecordHeaders();
        headers.add(TraceContextPropagator.HEADER_TRACE_ID, traceId.getBytes(StandardCharsets.UTF_8));
        headers.add(TraceContextPropagator.HEADER_SPAN_ID, spanId.getBytes(StandardCharsets.UTF_8));

        TraceContext context = TraceContextPropagator.extract(headers);

        assertThat(context).isNotNull();
        assertThat(context.getTraceId()).isEqualTo(traceId);
        assertThat(context.getSpanId()).isEqualTo(spanId);
        assertThat(context.getParentSpanId()).isNull();
    }

    @Test
    void extract_shouldExtractChildContext() {
        String traceId = UUID.randomUUID().toString();
        String spanId = UUID.randomUUID().toString();
        String parentSpanId = UUID.randomUUID().toString();

        Headers headers = new RecordHeaders();
        headers.add(TraceContextPropagator.HEADER_TRACE_ID, traceId.getBytes(StandardCharsets.UTF_8));
        headers.add(TraceContextPropagator.HEADER_SPAN_ID, spanId.getBytes(StandardCharsets.UTF_8));
        headers.add(TraceContextPropagator.HEADER_PARENT_SPAN_ID, parentSpanId.getBytes(StandardCharsets.UTF_8));

        TraceContext context = TraceContextPropagator.extract(headers);

        assertThat(context).isNotNull();
        assertThat(context.getTraceId()).isEqualTo(traceId);
        assertThat(context.getSpanId()).isEqualTo(spanId);
        assertThat(context.getParentSpanId()).isEqualTo(parentSpanId);
    }

    @Test
    void extract_shouldUseLastHeaderWhenDuplicates() {
        String firstTrace = UUID.randomUUID().toString();
        String secondTrace = UUID.randomUUID().toString();
        String spanId = UUID.randomUUID().toString();

        Headers headers = new RecordHeaders();
        headers.add(TraceContextPropagator.HEADER_TRACE_ID, firstTrace.getBytes(StandardCharsets.UTF_8));
        headers.add(TraceContextPropagator.HEADER_TRACE_ID, secondTrace.getBytes(StandardCharsets.UTF_8));
        headers.add(TraceContextPropagator.HEADER_SPAN_ID, spanId.getBytes(StandardCharsets.UTF_8));

        TraceContext context = TraceContextPropagator.extract(headers);

        assertThat(context.getTraceId()).isEqualTo(secondTrace);
    }

    @Test
    void inject_withNull_shouldThrowNullPointerException() {
        Headers headers = new RecordHeaders();

        assertThatThrownBy(() -> TraceContextPropagator.inject(null, headers))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("context must not be null");
    }

    @Test
    void inject_withNullHeaders_shouldThrowNullPointerException() {
        TraceContext context = TraceContext.root(UUID.randomUUID().toString(), UUID.randomUUID().toString());

        assertThatThrownBy(() -> TraceContextPropagator.inject(context, null))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("headers must not be null");
    }

    @Test
    void inject_shouldAddTraceIdAndSpanId() {
        TraceContext context = TraceContext.root(UUID.randomUUID().toString(), UUID.randomUUID().toString());
        Headers headers = new RecordHeaders();

        TraceContextPropagator.inject(context, headers);

        String traceId = new String(headers.lastHeader(TraceContextPropagator.HEADER_TRACE_ID).value(), StandardCharsets.UTF_8);
        String spanId = new String(headers.lastHeader(TraceContextPropagator.HEADER_SPAN_ID).value(), StandardCharsets.UTF_8);

        assertThat(traceId).isEqualTo(context.getTraceId());
        assertThat(spanId).isEqualTo(context.getSpanId());
    }

    @Test
    void inject_withRootContext_shouldNotAddParentSpanId() {
        TraceContext context = TraceContext.root(UUID.randomUUID().toString(), UUID.randomUUID().toString());
        Headers headers = new RecordHeaders();

        TraceContextPropagator.inject(context, headers);

        assertThat(headers.lastHeader(TraceContextPropagator.HEADER_PARENT_SPAN_ID)).isNull();
    }

    @Test
    void inject_withChildContext_shouldAddParentSpanId() {
        TraceContext context = TraceContext.child(
                UUID.randomUUID().toString(),
                UUID.randomUUID().toString(),
                UUID.randomUUID().toString()
        );
        Headers headers = new RecordHeaders();

        TraceContextPropagator.inject(context, headers);

        String parentSpanId = new String(
                headers.lastHeader(TraceContextPropagator.HEADER_PARENT_SPAN_ID).value(),
                StandardCharsets.UTF_8
        );
        assertThat(parentSpanId).isEqualTo(context.getParentSpanId());
    }

    @Test
    void roundTrip_shouldPreserveContext() {
        TraceContext original = TraceContext.child(
                UUID.randomUUID().toString(),
                UUID.randomUUID().toString(),
                UUID.randomUUID().toString()
        );
        Headers headers = new RecordHeaders();

        TraceContextPropagator.inject(original, headers);
        TraceContext extracted = TraceContextPropagator.extract(headers);

        assertThat(extracted).isEqualTo(original);
    }

    @Test
    void roundTrip_shouldPreserveRootContext() {
        TraceContext original = TraceContext.root(UUID.randomUUID().toString(), UUID.randomUUID().toString());
        Headers headers = new RecordHeaders();

        TraceContextPropagator.inject(original, headers);
        TraceContext extracted = TraceContextPropagator.extract(headers);

        assertThat(extracted).isEqualTo(original);
        assertThat(extracted.getParentSpanId()).isNull();
    }

    @Test
    void extractAndBind_withNullRecord_shouldDoNothing() {
        TraceContextPropagator.extractAndBind(null);

        assertThat(TraceContext.current()).isNull();
        assertThat(MDC.get(KTraceMDC.TRACE_ID_KEY)).isNull();
    }

    @Test
    void extractAndBind_withNoHeaders_shouldDoNothing() {
        ConsumerRecord<String, String> record = new ConsumerRecord<>(
                "orders", 0, 100L, "key", "value"
        );

        TraceContextPropagator.extractAndBind(record);

        assertThat(TraceContext.current()).isNull();
        assertThat(MDC.get(KTraceMDC.TRACE_ID_KEY)).isNull();
    }

    @Test
    void extractAndBind_shouldSetThreadLocalAndMdc() {
        // Enable MDC for this test
        KTraceConfig.setInstance(new KTraceConfig(true));

        String traceId = UUID.randomUUID().toString();
        String spanId = UUID.randomUUID().toString();
        String parentSpanId = UUID.randomUUID().toString();

        Headers headers = new RecordHeaders();
        headers.add(TraceContextPropagator.HEADER_TRACE_ID, traceId.getBytes(StandardCharsets.UTF_8));
        headers.add(TraceContextPropagator.HEADER_SPAN_ID, spanId.getBytes(StandardCharsets.UTF_8));
        headers.add(TraceContextPropagator.HEADER_PARENT_SPAN_ID, parentSpanId.getBytes(StandardCharsets.UTF_8));

        ConsumerRecord<String, String> record = new ConsumerRecord<String, String>(
                "orders", 0, 100L, System.currentTimeMillis(), TimestampType.CREATE_TIME,
                (long) -1, -1, -1, "key", "value", headers, Optional.empty()
        );

        TraceContextPropagator.extractAndBind(record);

        // Check ThreadLocal
        TraceContext current = TraceContext.current();
        assertThat(current).isNotNull();
        assertThat(current.getTraceId()).isEqualTo(traceId);
        assertThat(current.getSpanId()).isEqualTo(spanId);
        assertThat(current.getParentSpanId()).isEqualTo(parentSpanId);

        // Check MDC
        assertThat(MDC.get(KTraceMDC.TRACE_ID_KEY)).isEqualTo(traceId);
        assertThat(MDC.get(KTraceMDC.SPAN_ID_KEY)).isEqualTo(spanId);
        assertThat(MDC.get(KTraceMDC.PARENT_SPAN_ID_KEY)).isEqualTo(parentSpanId);
    }

    @Test
    void extractAndBind_withRootContext_shouldNotSetParentSpanIdInMdc() {
        String traceId = UUID.randomUUID().toString();
        String spanId = UUID.randomUUID().toString();

        Headers headers = new RecordHeaders();
        headers.add(TraceContextPropagator.HEADER_TRACE_ID, traceId.getBytes(StandardCharsets.UTF_8));
        headers.add(TraceContextPropagator.HEADER_SPAN_ID, spanId.getBytes(StandardCharsets.UTF_8));

        ConsumerRecord<String, String> record = new ConsumerRecord<String, String>(
                "orders", 0, 100L, System.currentTimeMillis(), TimestampType.CREATE_TIME,
                (long) -1, -1, -1, "key", "value", headers, Optional.empty()
        );

        TraceContextPropagator.extractAndBind(record);

        assertThat(MDC.get(KTraceMDC.PARENT_SPAN_ID_KEY)).isNull();
    }

    // Extract or Create Tests

    @Test
    void extractOrCreateAndBind_withHeaders_shouldExtract() {
        // Enable MDC for this test
        KTraceConfig.setInstance(new KTraceConfig(true));

        String traceId = UUID.randomUUID().toString();
        String spanId = UUID.randomUUID().toString();
        String parentSpanId = UUID.randomUUID().toString();

        Headers headers = new RecordHeaders();
        headers.add(TraceContextPropagator.HEADER_TRACE_ID, traceId.getBytes(StandardCharsets.UTF_8));
        headers.add(TraceContextPropagator.HEADER_SPAN_ID, spanId.getBytes(StandardCharsets.UTF_8));
        headers.add(TraceContextPropagator.HEADER_PARENT_SPAN_ID, parentSpanId.getBytes(StandardCharsets.UTF_8));

        ConsumerRecord<String, String> record = new ConsumerRecord<String, String>(
                "orders", 0, 100L, System.currentTimeMillis(), TimestampType.CREATE_TIME,
                (long) -1, -1, -1, "key", "value", headers, Optional.empty()
        );

        TraceContextPropagator.extractOrCreateAndBind(record);

        // Check ThreadLocal
        TraceContext current = TraceContext.current();
        assertThat(current).isNotNull();
        assertThat(current.getTraceId()).isEqualTo(traceId);
        assertThat(current.getSpanId()).isEqualTo(spanId);
        assertThat(current.getParentSpanId()).isEqualTo(parentSpanId);

        // Check MDC
        assertThat(MDC.get(KTraceMDC.TRACE_ID_KEY)).isEqualTo(traceId);
        assertThat(MDC.get(KTraceMDC.SPAN_ID_KEY)).isEqualTo(spanId);
        assertThat(MDC.get(KTraceMDC.PARENT_SPAN_ID_KEY)).isEqualTo(parentSpanId);
    }

    @Test
    void extractOrCreateAndBind_withoutHeaders_shouldCreateRoot() {
        // Enable MDC for this test
        KTraceConfig.setInstance(new KTraceConfig(true));

        ConsumerRecord<String, String> record = new ConsumerRecord<String, String>(
                "orders", 0, 100L, System.currentTimeMillis(), TimestampType.CREATE_TIME,
                (long) -1, -1, -1, "key", "value"
        );

        TraceContextPropagator.extractOrCreateAndBind(record);

        // Check ThreadLocal: should have root context
        TraceContext current = TraceContext.current();
        assertThat(current).isNotNull();
        assertThat(current.getTraceId()).isNotNull();
        assertThat(current.getSpanId()).isNotNull();
        assertThat(current.getParentSpanId()).isNull();  // Root has no parent

        // Check MDC
        assertThat(MDC.get(KTraceMDC.TRACE_ID_KEY)).isEqualTo(current.getTraceId());
        assertThat(MDC.get(KTraceMDC.SPAN_ID_KEY)).isEqualTo(current.getSpanId());
        assertThat(MDC.get(KTraceMDC.PARENT_SPAN_ID_KEY)).isNull();
    }

    @Test
    void extractOrCreateAndBind_withNullRecord_shouldDoNothing() {
        TraceContextPropagator.extractOrCreateAndBind(null);

        assertThat(TraceContext.current()).isNull();
        assertThat(MDC.get(KTraceMDC.TRACE_ID_KEY)).isNull();
    }

    @Test
    void extractOrCreateAndBind_shouldCreateValidUuids() {
        ConsumerRecord<String, String> record = new ConsumerRecord<String, String>(
                "orders", 0, 100L, System.currentTimeMillis(), TimestampType.CREATE_TIME,
                (long) -1, -1, -1, "key", "value"
        );

        TraceContextPropagator.extractOrCreateAndBind(record);

        TraceContext current = TraceContext.current();

        // Should not throw
        UUID.fromString(current.getTraceId());
        UUID.fromString(current.getSpanId());
    }
}
