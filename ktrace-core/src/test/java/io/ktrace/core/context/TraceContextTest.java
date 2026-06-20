package io.ktrace.core.context;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.*;

class TraceContextTest {

    @AfterEach
    void cleanup() {
        TraceContext.clearCurrent();
        assertThat(TraceContext.current()).isNull();
        assertThat(TraceContext.getTrigger()).isNull();
    }

    @Test
    void constructor_shouldCreateValidContext() {
        String traceId = UUID.randomUUID().toString();
        String spanId = UUID.randomUUID().toString();
        String parentSpanId = UUID.randomUUID().toString();

        TraceContext context = new TraceContext(traceId, spanId, parentSpanId);

        assertThat(context.getTraceId()).isEqualTo(traceId);
        assertThat(context.getSpanId()).isEqualTo(spanId);
        assertThat(context.getParentSpanId()).isEqualTo(parentSpanId);
    }

    @Test
    void root_shouldCreateContextWithNullParent() {
        String traceId = UUID.randomUUID().toString();
        String spanId = UUID.randomUUID().toString();

        TraceContext context = TraceContext.root(traceId, spanId);

        assertThat(context.getTraceId()).isEqualTo(traceId);
        assertThat(context.getSpanId()).isEqualTo(spanId);
        assertThat(context.getParentSpanId()).isNull();
    }

    @Test
    void child_shouldCreateContextWithParent() {
        String traceId = UUID.randomUUID().toString();
        String spanId = UUID.randomUUID().toString();
        String parentSpanId = UUID.randomUUID().toString();

        TraceContext context = TraceContext.child(traceId, spanId, parentSpanId);

        assertThat(context.getTraceId()).isEqualTo(traceId);
        assertThat(context.getSpanId()).isEqualTo(spanId);
        assertThat(context.getParentSpanId()).isEqualTo(parentSpanId);
    }

    @Test
    void constructor_shouldRequireTraceId() {
        assertThatThrownBy(() -> new TraceContext(null, UUID.randomUUID().toString(), null))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("traceId must not be null");
    }

    @Test
    void constructor_shouldRequireSpanId() {
        assertThatThrownBy(() -> new TraceContext(UUID.randomUUID().toString(), null, null))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("spanId must not be null");
    }

    @Test
    void constructor_shouldValidateTraceIdUuid() {
        assertThatThrownBy(() -> new TraceContext("not-a-uuid", UUID.randomUUID().toString(), null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("traceId must be a valid UUID");
    }

    @Test
    void constructor_shouldValidateSpanIdUuid() {
        assertThatThrownBy(() -> new TraceContext(UUID.randomUUID().toString(), "not-a-uuid", null))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("spanId must be a valid UUID");
    }

    @Test
    void constructor_shouldValidateParentSpanIdUuid() {
        assertThatThrownBy(() -> new TraceContext(
                UUID.randomUUID().toString(),
                UUID.randomUUID().toString(),
                "not-a-uuid"
        ))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("parentSpanId must be a valid UUID");
    }

    @Test
    void constructor_shouldAllowNullParentSpanId() {
        String traceId = UUID.randomUUID().toString();
        String spanId = UUID.randomUUID().toString();

        TraceContext context = new TraceContext(traceId, spanId, null);

        assertThat(context.getParentSpanId()).isNull();
    }

    @Test
    void current_shouldReturnNullWhenNotSet() {
        assertThat(TraceContext.current()).isNull();
    }

    @Test
    void setCurrent_shouldStoreContextInThreadLocal() {
        TraceContext context = TraceContext.root(UUID.randomUUID().toString(), UUID.randomUUID().toString());

        TraceContext.setCurrent(context);

        assertThat(TraceContext.current()).isEqualTo(context);
    }

    @Test
    void setCurrent_shouldOverridePreviousContext() {
        TraceContext first = TraceContext.root(UUID.randomUUID().toString(), UUID.randomUUID().toString());
        TraceContext second = TraceContext.root(UUID.randomUUID().toString(), UUID.randomUUID().toString());

        TraceContext.setCurrent(first);
        assertThat(TraceContext.current()).isEqualTo(first);

        TraceContext.setCurrent(second);
        assertThat(TraceContext.current()).isEqualTo(second);
    }

    @Test
    void setCurrent_withNull_shouldClearContext() {
        TraceContext context = TraceContext.root(UUID.randomUUID().toString(), UUID.randomUUID().toString());
        TraceContext.setCurrent(context);

        TraceContext.setCurrent(null);

        assertThat(TraceContext.current()).isNull();
    }

    @Test
    void clearCurrent_shouldRemoveContextFromThreadLocal() {
        TraceContext context = TraceContext.root(UUID.randomUUID().toString(), UUID.randomUUID().toString());
        TraceContext.setCurrent(context);

        TraceContext.clearCurrent();

        assertThat(TraceContext.current()).isNull();
    }

    @Test
    void threadLocal_shouldIsolateContextsAcrossThreads() throws Exception {
        TraceContext mainContext = TraceContext.root(UUID.randomUUID().toString(), UUID.randomUUID().toString());
        TraceContext.setCurrent(mainContext);

        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<TraceContext> otherThreadContext = new AtomicReference<>();

        Thread otherThread = new Thread(() -> {
            otherThreadContext.set(TraceContext.current());
            latch.countDown();
        });

        otherThread.start();
        latch.await();

        assertThat(TraceContext.current()).isEqualTo(mainContext);
        assertThat(otherThreadContext.get()).isNull();
    }

    @Test
    void setTrigger_shouldStoreTriggerMetadata() {
        TraceContext.setTrigger("orders", 2, 100L, "order-processor-group");

        TraceContext.TriggerMetadata trigger = TraceContext.getTrigger();

        assertThat(trigger).isNotNull();
        assertThat(trigger.getTopic()).isEqualTo("orders");
        assertThat(trigger.getPartition()).isEqualTo(2);
        assertThat(trigger.getOffset()).isEqualTo(100L);
        assertThat(trigger.getGroup()).isEqualTo("order-processor-group");
    }

    @Test
    void setTrigger_shouldAllowNullGroup() {
        TraceContext.setTrigger("orders", 2, 100L, null);

        TraceContext.TriggerMetadata trigger = TraceContext.getTrigger();

        assertThat(trigger).isNotNull();
        assertThat(trigger.getGroup()).isNull();
    }

    @Test
    void getTrigger_shouldReturnNullWhenNotSet() {
        assertThat(TraceContext.getTrigger()).isNull();
    }

    @Test
    void clearCurrent_shouldClearTriggerMetadata() {
        TraceContext.setTrigger("orders", 2, 100L, "group");

        TraceContext.clearCurrent();

        assertThat(TraceContext.getTrigger()).isNull();
    }

    @Test
    void equals_shouldReturnTrueForSameData() {
        String traceId = UUID.randomUUID().toString();
        String spanId = UUID.randomUUID().toString();
        String parentSpanId = UUID.randomUUID().toString();

        TraceContext context1 = new TraceContext(traceId, spanId, parentSpanId);
        TraceContext context2 = new TraceContext(traceId, spanId, parentSpanId);

        assertThat(context1).isEqualTo(context2);
        assertThat(context1.hashCode()).isEqualTo(context2.hashCode());
    }

    @Test
    void equals_shouldReturnFalseForDifferentTraceId() {
        String spanId = UUID.randomUUID().toString();

        TraceContext context1 = new TraceContext(UUID.randomUUID().toString(), spanId, null);
        TraceContext context2 = new TraceContext(UUID.randomUUID().toString(), spanId, null);

        assertThat(context1).isNotEqualTo(context2);
    }

    @Test
    void equals_shouldReturnFalseForDifferentSpanId() {
        String traceId = UUID.randomUUID().toString();

        TraceContext context1 = new TraceContext(traceId, UUID.randomUUID().toString(), null);
        TraceContext context2 = new TraceContext(traceId, UUID.randomUUID().toString(), null);

        assertThat(context1).isNotEqualTo(context2);
    }

    @Test
    void toString_shouldIncludeAllFields() {
        String traceId = UUID.randomUUID().toString();
        String spanId = UUID.randomUUID().toString();
        String parentSpanId = UUID.randomUUID().toString();

        TraceContext context = new TraceContext(traceId, spanId, parentSpanId);

        String string = context.toString();
        assertThat(string).contains(traceId);
        assertThat(string).contains(spanId);
        assertThat(string).contains(parentSpanId);
    }

    @Test
    void currentOrCreateRoot_shouldReturnExistingContext() {
        TraceContext context = TraceContext.root(UUID.randomUUID().toString(), UUID.randomUUID().toString());
        TraceContext.setCurrent(context);

        TraceContext result = TraceContext.currentOrCreateRoot();

        assertThat(result).isSameAs(context);
    }

    @Test
    void currentOrCreateRoot_shouldCreateRootWhenNone() {
        assertThat(TraceContext.current()).isNull();

        TraceContext result = TraceContext.currentOrCreateRoot();

        assertThat(result).isNotNull();
        assertThat(result.getParentSpanId()).isNull();
        assertThat(result.getTraceId()).isNotNull();
        assertThat(result.getSpanId()).isNotNull();
    }

    @Test
    void currentOrCreateRoot_shouldStoreInThreadLocal() {
        TraceContext result = TraceContext.currentOrCreateRoot();

        TraceContext retrieved = TraceContext.current();

        assertThat(retrieved).isSameAs(result);
    }

    @Test
    void currentOrCreateRoot_shouldGenerateValidUuids() {
        TraceContext result = TraceContext.currentOrCreateRoot();

        // Should not throw exception
        UUID.fromString(result.getTraceId());
        UUID.fromString(result.getSpanId());
    }

    @Test
    void currentOrCreateRoot_calledTwice_shouldReturnSameContext() {
        TraceContext first = TraceContext.currentOrCreateRoot();
        TraceContext second = TraceContext.currentOrCreateRoot();

        assertThat(first).isSameAs(second);
    }
}
