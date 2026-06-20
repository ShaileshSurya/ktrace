package io.ktrace.core.helper;

import io.ktrace.core.config.KTraceConfig;
import io.ktrace.core.context.KTraceMDC;
import io.ktrace.core.context.TraceContext;
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

import static org.assertj.core.api.Assertions.*;

class KTraceHelperTest {

    private final KTraceConfig originalConfig = KTraceConfig.getInstance();

    @AfterEach
    void cleanup() {
        TraceContext.clearCurrent();
        KTraceMDC.clear();
        KTraceConfig.setInstance(originalConfig);
    }

    @Test
    void executeWithTrace_withHeadersAndHandler_shouldSetAndCleanup() throws Exception {
        // Enable MDC for this test
        KTraceConfig.setInstance(new KTraceConfig(true));

        String traceId = UUID.randomUUID().toString();
        String spanId = UUID.randomUUID().toString();

        Headers headers = new RecordHeaders();
        headers.add("ktrace-trace-id", traceId.getBytes(StandardCharsets.UTF_8));
        headers.add("ktrace-span-id", spanId.getBytes(StandardCharsets.UTF_8));

        ConsumerRecord<String, String> record = new ConsumerRecord<String, String>(
                "orders", 0, 100L, System.currentTimeMillis(), TimestampType.CREATE_TIME,
                (long) -1, -1, -1, "key", "value", headers, Optional.empty()
        );

        KTraceHelper.executeWithTrace(record, r -> {
            // Inside handler: context should be set
            assertThat(TraceContext.current()).isNotNull();
            assertThat(TraceContext.current().getTraceId()).isEqualTo(traceId);
            assertThat(MDC.get(KTraceMDC.TRACE_ID_KEY)).isEqualTo(traceId);
            return null;
        });

        // After handler: context should be cleaned up
        assertThat(TraceContext.current()).isNull();
        assertThat(MDC.get(KTraceMDC.TRACE_ID_KEY)).isNull();
    }

    @Test
    void executeWithTrace_withoutHeaders_shouldCreateRoot() throws Exception {
        ConsumerRecord<String, String> record = new ConsumerRecord<String, String>(
                "orders", 0, 100L, System.currentTimeMillis(), TimestampType.CREATE_TIME,
                (long) -1, -1, -1, "key", "value"
        );

        KTraceHelper.executeWithTrace(record, r -> {
            // Inside handler: root context should be created
            TraceContext ctx = TraceContext.current();
            assertThat(ctx).isNotNull();
            assertThat(ctx.getTraceId()).isNotNull();
            assertThat(ctx.getSpanId()).isNotNull();
            assertThat(ctx.getParentSpanId()).isNull();  // Root
            return null;
        });

        // After handler: cleaned up
        assertThat(TraceContext.current()).isNull();
    }

    @Test
    void executeWithTrace_withException_shouldStillCleanup() {
        ConsumerRecord<String, String> record = new ConsumerRecord<String, String>(
                "orders", 0, 100L, System.currentTimeMillis(), TimestampType.CREATE_TIME,
                (long) -1, -1, -1, "key", "value"
        );

        assertThatThrownBy(() ->
            KTraceHelper.executeWithTrace(record, r -> {
                TraceContext.setCurrent(TraceContext.root(
                    UUID.randomUUID().toString(),
                    UUID.randomUUID().toString()
                ));
                throw new RuntimeException("Test exception");
            })
        ).isInstanceOf(RuntimeException.class);

        // Even with exception: cleanup happened
        assertThat(TraceContext.current()).isNull();
        assertThat(MDC.get(KTraceMDC.TRACE_ID_KEY)).isNull();
    }

    @Test
    void executeWithTrace_withNullRecord_shouldThrowNullPointerException() {
        assertThatThrownBy(() ->
            KTraceHelper.executeWithTrace(null, r -> null)
        ).isInstanceOf(NullPointerException.class)
         .hasMessageContaining("record must not be null");
    }

    @Test
    void executeWithTrace_withNullHandler_shouldThrowNullPointerException() throws Exception {
        ConsumerRecord<String, String> record = new ConsumerRecord<String, String>(
                "orders", 0, 100L, System.currentTimeMillis(), TimestampType.CREATE_TIME,
                (long) -1, -1, -1, "key", "value"
        );

        assertThatThrownBy(() ->
            KTraceHelper.executeWithTrace(record, null)
        ).isInstanceOf(NullPointerException.class)
         .hasMessageContaining("handler must not be null");
    }

    @Test
    void executeWithTrace_shouldReturnHandlerResult() throws Exception {
        ConsumerRecord<String, String> record = new ConsumerRecord<String, String>(
                "orders", 0, 100L, System.currentTimeMillis(), TimestampType.CREATE_TIME,
                (long) -1, -1, -1, "key", "value"
        );

        String result = KTraceHelper.executeWithTrace(record, r ->
            "processed: " + r.value()
        );

        assertThat(result).isEqualTo("processed: value");
    }

    @Test
    void executeWithTrace_withMultipleCalls_shouldIsolatContexts() throws Exception {
        ConsumerRecord<String, String> record1 = new ConsumerRecord<String, String>(
                "topic1", 0, 100L, System.currentTimeMillis(), TimestampType.CREATE_TIME,
                (long) -1, -1, -1, "key1", "value1"
        );

        ConsumerRecord<String, String> record2 = new ConsumerRecord<String, String>(
                "topic2", 0, 200L, System.currentTimeMillis(), TimestampType.CREATE_TIME,
                (long) -1, -1, -1, "key2", "value2"
        );

        // First call
        String traceId1 = KTraceHelper.executeWithTrace(record1, r -> {
            return TraceContext.current().getTraceId();
        });

        // Between calls: context cleaned
        assertThat(TraceContext.current()).isNull();

        // Second call
        String traceId2 = KTraceHelper.executeWithTrace(record2, r -> {
            return TraceContext.current().getTraceId();
        });

        // Each call had different context
        assertThat(traceId1).isNotEqualTo(traceId2);
    }

    @Test
    void executeWithTrace_shouldPreserveUserMdcKeys() throws Exception {
        MDC.put("userId", "user-123");

        ConsumerRecord<String, String> record = new ConsumerRecord<String, String>(
                "orders", 0, 100L, System.currentTimeMillis(), TimestampType.CREATE_TIME,
                (long) -1, -1, -1, "key", "value"
        );

        KTraceHelper.executeWithTrace(record, r -> {
            // User key should be accessible inside handler
            assertThat(MDC.get("userId")).isEqualTo("user-123");
            return null;
        });

        // User key should still be there after cleanup
        assertThat(MDC.get("userId")).isEqualTo("user-123");

        // Cleanup user key
        MDC.remove("userId");
    }
}
