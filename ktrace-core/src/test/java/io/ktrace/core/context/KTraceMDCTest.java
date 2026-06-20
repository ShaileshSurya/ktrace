package io.ktrace.core.context;

import io.ktrace.core.config.KTraceConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.MDC;

import java.util.UUID;

import static org.assertj.core.api.Assertions.*;

class KTraceMDCTest {

    private final KTraceConfig originalConfig = KTraceConfig.getInstance();

    @BeforeEach
    void setupConfig() {
        KTraceConfig.setInstance(new KTraceConfig(true));  // Enable logging for tests
    }

    @AfterEach
    void cleanup() {
        KTraceMDC.clear();
        TraceContext.clearCurrent();
        KTraceConfig.setInstance(originalConfig);

        // Verify no MDC leak (Scenario 8 from spec)
        assertThat(MDC.get(KTraceMDC.TRACE_ID_KEY))
                .as("ktrace.traceId should be null after test")
                .isNull();
        assertThat(MDC.get(KTraceMDC.SPAN_ID_KEY))
                .as("ktrace.spanId should be null after test")
                .isNull();
        assertThat(MDC.get(KTraceMDC.PARENT_SPAN_ID_KEY))
                .as("ktrace.parentSpanId should be null after test")
                .isNull();
    }

    @Test
    void put_shouldSetTraceIdAndSpanId() {
        TraceContext context = TraceContext.root(
                UUID.randomUUID().toString(),
                UUID.randomUUID().toString()
        );

        KTraceMDC.put(context);

        assertThat(MDC.get(KTraceMDC.TRACE_ID_KEY)).isEqualTo(context.getTraceId());
        assertThat(MDC.get(KTraceMDC.SPAN_ID_KEY)).isEqualTo(context.getSpanId());
    }

    @Test
    void put_withRootContext_shouldNotSetParentSpanId() {
        TraceContext context = TraceContext.root(
                UUID.randomUUID().toString(),
                UUID.randomUUID().toString()
        );

        KTraceMDC.put(context);

        assertThat(MDC.get(KTraceMDC.PARENT_SPAN_ID_KEY)).isNull();
    }

    @Test
    void put_withChildContext_shouldSetParentSpanId() {
        TraceContext context = TraceContext.child(
                UUID.randomUUID().toString(),
                UUID.randomUUID().toString(),
                UUID.randomUUID().toString()
        );

        KTraceMDC.put(context);

        assertThat(MDC.get(KTraceMDC.TRACE_ID_KEY)).isEqualTo(context.getTraceId());
        assertThat(MDC.get(KTraceMDC.SPAN_ID_KEY)).isEqualTo(context.getSpanId());
        assertThat(MDC.get(KTraceMDC.PARENT_SPAN_ID_KEY)).isEqualTo(context.getParentSpanId());
    }

    @Test
    void put_withNull_shouldThrowNullPointerException() {
        assertThatThrownBy(() -> KTraceMDC.put(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("context must not be null");
    }

    @Test
    void put_shouldOverridePreviousContext() {
        TraceContext first = TraceContext.root(UUID.randomUUID().toString(), UUID.randomUUID().toString());
        TraceContext second = TraceContext.root(UUID.randomUUID().toString(), UUID.randomUUID().toString());

        KTraceMDC.put(first);
        assertThat(MDC.get(KTraceMDC.TRACE_ID_KEY)).isEqualTo(first.getTraceId());

        KTraceMDC.put(second);
        assertThat(MDC.get(KTraceMDC.TRACE_ID_KEY)).isEqualTo(second.getTraceId());
    }

    @Test
    void put_shouldRemoveParentSpanIdWhenSwitchingToRootContext() {
        TraceContext child = TraceContext.child(
                UUID.randomUUID().toString(),
                UUID.randomUUID().toString(),
                UUID.randomUUID().toString()
        );
        KTraceMDC.put(child);
        assertThat(MDC.get(KTraceMDC.PARENT_SPAN_ID_KEY)).isNotNull();

        TraceContext root = TraceContext.root(
                UUID.randomUUID().toString(),
                UUID.randomUUID().toString()
        );
        KTraceMDC.put(root);

        assertThat(MDC.get(KTraceMDC.PARENT_SPAN_ID_KEY)).isNull();
    }

    @Test
    void clear_shouldRemoveAllKtraceKeys() {
        TraceContext context = TraceContext.child(
                UUID.randomUUID().toString(),
                UUID.randomUUID().toString(),
                UUID.randomUUID().toString()
        );
        KTraceMDC.put(context);

        KTraceMDC.clear();

        assertThat(MDC.get(KTraceMDC.TRACE_ID_KEY)).isNull();
        assertThat(MDC.get(KTraceMDC.SPAN_ID_KEY)).isNull();
        assertThat(MDC.get(KTraceMDC.PARENT_SPAN_ID_KEY)).isNull();
    }

    @Test
    void clear_shouldPreserveOtherMdcKeys() {
        // Scenario 9 from spec: selective clear
        MDC.put("requestId", "req-123");
        MDC.put("userId", "user-456");
        MDC.put("sessionId", "session-789");

        TraceContext context = TraceContext.child(
                UUID.randomUUID().toString(),
                UUID.randomUUID().toString(),
                UUID.randomUUID().toString()
        );
        KTraceMDC.put(context);

        KTraceMDC.clear();

        // ktrace.* keys removed
        assertThat(MDC.get(KTraceMDC.TRACE_ID_KEY)).isNull();
        assertThat(MDC.get(KTraceMDC.SPAN_ID_KEY)).isNull();
        assertThat(MDC.get(KTraceMDC.PARENT_SPAN_ID_KEY)).isNull();

        // Other keys preserved
        assertThat(MDC.get("requestId")).isEqualTo("req-123");
        assertThat(MDC.get("userId")).isEqualTo("user-456");
        assertThat(MDC.get("sessionId")).isEqualTo("session-789");

        // Clean up test keys
        MDC.remove("requestId");
        MDC.remove("userId");
        MDC.remove("sessionId");
    }

    @Test
    void clear_shouldBeIdempotent() {
        TraceContext context = TraceContext.root(UUID.randomUUID().toString(), UUID.randomUUID().toString());
        KTraceMDC.put(context);

        KTraceMDC.clear();
        KTraceMDC.clear();

        assertThat(MDC.get(KTraceMDC.TRACE_ID_KEY)).isNull();
    }

    @Test
    void current_shouldReturnNullWhenNotSet() {
        assertThat(KTraceMDC.current()).isNull();
    }

    @Test
    void current_shouldReadContextFromMdc() {
        TraceContext original = TraceContext.child(
                UUID.randomUUID().toString(),
                UUID.randomUUID().toString(),
                UUID.randomUUID().toString()
        );
        KTraceMDC.put(original);

        TraceContext fromMdc = KTraceMDC.current();

        assertThat(fromMdc).isNotNull();
        assertThat(fromMdc.getTraceId()).isEqualTo(original.getTraceId());
        assertThat(fromMdc.getSpanId()).isEqualTo(original.getSpanId());
        assertThat(fromMdc.getParentSpanId()).isEqualTo(original.getParentSpanId());
    }

    @Test
    void current_shouldReturnNullWhenOnlyTraceIdSet() {
        MDC.put(KTraceMDC.TRACE_ID_KEY, UUID.randomUUID().toString());

        TraceContext context = KTraceMDC.current();

        assertThat(context).isNull();

        MDC.remove(KTraceMDC.TRACE_ID_KEY);
    }

    @Test
    void current_shouldHandleRootContext() {
        TraceContext root = TraceContext.root(
                UUID.randomUUID().toString(),
                UUID.randomUUID().toString()
        );
        KTraceMDC.put(root);

        TraceContext fromMdc = KTraceMDC.current();

        assertThat(fromMdc).isNotNull();
        assertThat(fromMdc.getTraceId()).isEqualTo(root.getTraceId());
        assertThat(fromMdc.getSpanId()).isEqualTo(root.getSpanId());
        assertThat(fromMdc.getParentSpanId()).isNull();
    }

    @Test
    void scoped_shouldSetAndClearMdc() {
        TraceContext context = TraceContext.root(UUID.randomUUID().toString(), UUID.randomUUID().toString());

        KTraceMDC.scoped(context, () -> {
            assertThat(MDC.get(KTraceMDC.TRACE_ID_KEY)).isEqualTo(context.getTraceId());
            assertThat(MDC.get(KTraceMDC.SPAN_ID_KEY)).isEqualTo(context.getSpanId());
        });

        assertThat(MDC.get(KTraceMDC.TRACE_ID_KEY)).isNull();
        assertThat(MDC.get(KTraceMDC.SPAN_ID_KEY)).isNull();
    }

    @Test
    void scoped_shouldClearMdcEvenOnException() {
        // Scenario 6 from spec: exception safety
        TraceContext context = TraceContext.root(UUID.randomUUID().toString(), UUID.randomUUID().toString());

        assertThatThrownBy(() ->
                KTraceMDC.scoped(context, () -> {
                    assertThat(MDC.get(KTraceMDC.TRACE_ID_KEY)).isEqualTo(context.getTraceId());
                    throw new RuntimeException("Test exception");
                })
        ).isInstanceOf(RuntimeException.class);

        // MDC should still be cleared
        assertThat(MDC.get(KTraceMDC.TRACE_ID_KEY)).isNull();
        assertThat(MDC.get(KTraceMDC.SPAN_ID_KEY)).isNull();
    }

    @Test
    void scoped_withNullContext_shouldThrowNullPointerException() {
        assertThatThrownBy(() -> KTraceMDC.scoped(null, () -> {}))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("context must not be null");
    }

    @Test
    void scoped_withNullRunnable_shouldThrowNullPointerException() {
        TraceContext context = TraceContext.root(UUID.randomUUID().toString(), UUID.randomUUID().toString());

        assertThatThrownBy(() -> KTraceMDC.scoped(context, null))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("runnable must not be null");
    }

    @Test
    void scoped_shouldPreserveOtherMdcKeys() {
        MDC.put("requestId", "req-123");
        TraceContext context = TraceContext.root(UUID.randomUUID().toString(), UUID.randomUUID().toString());

        KTraceMDC.scoped(context, () -> {
            assertThat(MDC.get("requestId")).isEqualTo("req-123");
        });

        assertThat(MDC.get("requestId")).isEqualTo("req-123");

        MDC.remove("requestId");
    }

    @Test
    void threadLocal_shouldIsolateMdcAcrossThreads() throws Exception {
        // Scenario 7 from spec: thread isolation
        TraceContext mainContext = TraceContext.root(UUID.randomUUID().toString(), UUID.randomUUID().toString());
        KTraceMDC.put(mainContext);

        Thread otherThread = new Thread(() -> {
            assertThat(MDC.get(KTraceMDC.TRACE_ID_KEY)).isNull();
        });

        otherThread.start();
        otherThread.join();

        assertThat(MDC.get(KTraceMDC.TRACE_ID_KEY)).isEqualTo(mainContext.getTraceId());
    }

    // Configurable Logging Tests

    @Test
    void put_withLoggingDisabled_shouldNotSetMdc() {
        KTraceConfig.setInstance(new KTraceConfig(false));  // Disable logging

        TraceContext context = TraceContext.root(UUID.randomUUID().toString(), UUID.randomUUID().toString());
        KTraceMDC.put(context);

        // MDC should be empty (not set)
        assertThat(MDC.get(KTraceMDC.TRACE_ID_KEY)).isNull();
        assertThat(MDC.get(KTraceMDC.SPAN_ID_KEY)).isNull();
    }

    @Test
    void put_withLoggingEnabled_shouldSetMdc() {
        KTraceConfig.setInstance(new KTraceConfig(true));  // Enable logging

        TraceContext context = TraceContext.root(UUID.randomUUID().toString(), UUID.randomUUID().toString());
        KTraceMDC.put(context);

        // MDC should be set
        assertThat(MDC.get(KTraceMDC.TRACE_ID_KEY)).isEqualTo(context.getTraceId());
        assertThat(MDC.get(KTraceMDC.SPAN_ID_KEY)).isEqualTo(context.getSpanId());
    }

    @Test
    void clear_withLoggingDisabled_shouldDoNothing() {
        KTraceConfig.setInstance(new KTraceConfig(true));  // Start enabled

        TraceContext context = TraceContext.root(UUID.randomUUID().toString(), UUID.randomUUID().toString());
        KTraceMDC.put(context);  // Sets MDC

        KTraceConfig.setInstance(new KTraceConfig(false));  // Disable logging
        KTraceMDC.clear();  // Should skip clearing

        // MDC should still be there (clear was skipped)
        assertThat(MDC.get(KTraceMDC.TRACE_ID_KEY)).isEqualTo(context.getTraceId());

        // Manual cleanup for this test
        MDC.remove(KTraceMDC.TRACE_ID_KEY);
        MDC.remove(KTraceMDC.SPAN_ID_KEY);
    }

    @Test
    void clear_withLoggingEnabled_shouldClearMdc() {
        KTraceConfig.setInstance(new KTraceConfig(true));  // Enable logging

        TraceContext context = TraceContext.root(UUID.randomUUID().toString(), UUID.randomUUID().toString());
        KTraceMDC.put(context);  // Sets MDC

        KTraceMDC.clear();  // Should clear

        // MDC should be empty
        assertThat(MDC.get(KTraceMDC.TRACE_ID_KEY)).isNull();
        assertThat(MDC.get(KTraceMDC.SPAN_ID_KEY)).isNull();
    }

    @Test
    void scoped_withLoggingDisabled_shouldNotSetMdc() {
        KTraceConfig.setInstance(new KTraceConfig(false));  // Disable logging

        TraceContext context = TraceContext.root(UUID.randomUUID().toString(), UUID.randomUUID().toString());

        KTraceMDC.scoped(context, () -> {
            // Inside scoped block, MDC should be empty (logging disabled)
            assertThat(MDC.get(KTraceMDC.TRACE_ID_KEY)).isNull();
        });

        // After scoped, MDC should remain empty
        assertThat(MDC.get(KTraceMDC.TRACE_ID_KEY)).isNull();
    }

    @Test
    void scoped_withLoggingEnabled_shouldSetAndClearMdc() {
        KTraceConfig.setInstance(new KTraceConfig(true));  // Enable logging

        TraceContext context = TraceContext.root(UUID.randomUUID().toString(), UUID.randomUUID().toString());

        KTraceMDC.scoped(context, () -> {
            // Inside scoped block, MDC should be set
            assertThat(MDC.get(KTraceMDC.TRACE_ID_KEY)).isEqualTo(context.getTraceId());
        });

        // After scoped, MDC should be cleared
        assertThat(MDC.get(KTraceMDC.TRACE_ID_KEY)).isNull();
    }
}
