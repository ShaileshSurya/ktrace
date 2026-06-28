package io.ktrace.core.interceptor;

import io.ktrace.core.context.TraceContext;
import io.ktrace.core.context.TraceContextPropagator;
import io.ktrace.core.event.TraceEvent;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for Scenario 2: Consumer-Triggered Produce (Child Span).
 * <p>
 * These tests simulate the complete end-to-end flow:
 * 1. Consumer receives message with ktrace headers
 * 2. Consumer extracts context and sets trigger metadata
 * 3. Consumer produces new message
 * 4. Interceptor creates child span with proper causal links
 * <p>
 * This validates the specification requirements from interceptor-behavior.spec.md.
 */
class Scenario2IntegrationTest {

    private KTraceProducerInterceptor<String, String> interceptor;

    @BeforeEach
    void setUp() {
        Map<String, Object> config = new HashMap<>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put("ktrace.enabled", "true");
        config.put("ktrace.application-name", "order-processor");
        config.put("client.id", "test-producer");

        interceptor = new KTraceProducerInterceptor<>();
        interceptor.configure(config);
    }

    @AfterEach
    void tearDown() {
        if (interceptor != null) {
            interceptor.close();
        }
        TraceContext.clearCurrent();
    }

    @Test
    void scenario2_completeFlow_shouldMatchSpecification() {
        // ===== GIVEN: Consumer received message with ktrace headers =====

        // Simulate upstream producer sending to "orders" topic
        ConsumerRecord<String, String> consumedRecord = new ConsumerRecord<>(
                "orders",       // topic
                2,              // partition
                100L,           // offset
                "order-123",    // key
                "order-data"    // value
        );

        // Add ktrace headers (from upstream producer)
        consumedRecord.headers().add(
                TraceContextPropagator.HEADER_TRACE_ID,
                "550e8400-e29b-41d4-a716-446655440000".getBytes(StandardCharsets.UTF_8)
        );
        consumedRecord.headers().add(
                TraceContextPropagator.HEADER_SPAN_ID,
                "6ba7b810-9dad-11d1-80b4-00c04fd430c8".getBytes(StandardCharsets.UTF_8)
        );

        // ===== STEP 1: Consumer extracts and binds context =====
        TraceContextPropagator.extractAndBind(consumedRecord);

        // Verify ThreadLocal context is set correctly
        TraceContext consumerContext = TraceContext.current();
        assertThat(consumerContext).isNotNull();
        assertThat(consumerContext.getTraceId()).isEqualTo("550e8400-e29b-41d4-a716-446655440000");
        assertThat(consumerContext.getSpanId()).isEqualTo("6ba7b810-9dad-11d1-80b4-00c04fd430c8");
        assertThat(consumerContext.getParentSpanId()).isNull();  // Upstream was root span

        // ===== STEP 2: Consumer stores trigger metadata =====
        TraceContext.setTrigger("orders", 2, 100L, "order-processor-group");

        // Verify trigger metadata is stored
        TraceContext.TriggerMetadata trigger = TraceContext.getTrigger();
        assertThat(trigger).isNotNull();
        assertThat(trigger.getTopic()).isEqualTo("orders");
        assertThat(trigger.getPartition()).isEqualTo(2);
        assertThat(trigger.getOffset()).isEqualTo(100L);
        assertThat(trigger.getGroup()).isEqualTo("order-processor-group");

        // ===== WHEN: Consumer produces new message to "notifications" topic =====
        ProducerRecord<String, String> outgoingRecord = new ProducerRecord<>(
                "notifications",
                "notif-key",
                "notification-payload"
        );

        ProducerRecord<String, String> modifiedRecord = interceptor.onSend(outgoingRecord);

        // ===== THEN: Verify interceptor behavior matches specification =====

        // Step 1: Interceptor called TraceContext.current()
        // (verified above - consumer context exists)

        // Step 2: Interceptor called TraceContextPropagator.extract(record.headers())
        // (returns null because outgoing record has no headers initially)

        // Step 3: Used context from ThreadLocal (not from headers)

        // Step 4: Generated NEW spanId (different from consumer's spanId)
        String producerSpanId = new String(
                modifiedRecord.headers().lastHeader(TraceContextPropagator.HEADER_SPAN_ID).value(),
                StandardCharsets.UTF_8
        );
        assertThat(producerSpanId).isNotEqualTo("6ba7b810-9dad-11d1-80b4-00c04fd430c8");
        assertThat(producerSpanId).matches("[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}");

        // Step 5: Set parentSpanId = consumer's spanId (causal link)
        String parentSpanId = new String(
                modifiedRecord.headers().lastHeader(TraceContextPropagator.HEADER_PARENT_SPAN_ID).value(),
                StandardCharsets.UTF_8
        );
        assertThat(parentSpanId).isEqualTo("6ba7b810-9dad-11d1-80b4-00c04fd430c8");

        // Step 6: Reused traceId from consumer (chain preserved)
        String traceId = new String(
                modifiedRecord.headers().lastHeader(TraceContextPropagator.HEADER_TRACE_ID).value(),
                StandardCharsets.UTF_8
        );
        assertThat(traceId).isEqualTo("550e8400-e29b-41d4-a716-446655440000");

        // Step 7: Interceptor called TraceContext.getTrigger()
        // (verified by trigger metadata presence - tested implicitly via builder)

        // Step 8: Created new TraceContext (kept in local variable only)
        // Cannot verify directly, but headers prove it was created

        // Step 9: Injected ktrace headers into record
        assertThat(modifiedRecord.headers().lastHeader(TraceContextPropagator.HEADER_TRACE_ID)).isNotNull();
        assertThat(modifiedRecord.headers().lastHeader(TraceContextPropagator.HEADER_SPAN_ID)).isNotNull();
        assertThat(modifiedRecord.headers().lastHeader(TraceContextPropagator.HEADER_PARENT_SPAN_ID)).isNotNull();

        // Step 10: Built TraceEvent (published to __ktrace)
        // Note: TraceEvent fields verified implicitly through code review
        // Expected TraceEvent structure:
        // - traceId: "550e8400-e29b-41d4-a716-446655440000" (inherited)
        // - spanId: <new UUID> (unique producer span)
        // - parentSpanId: "6ba7b810-9dad-11d1-80b4-00c04fd430c8" (consumer's span)
        // - producedTopic: "notifications"
        // - producedPartition: -1 (not yet known)
        // - producedOffset: -1 (not yet known)
        // - triggerTopic: "orders"
        // - triggerPartition: 2
        // - triggerOffset: 100
        // - triggerConsumerGroup: "order-processor-group"
        // - producerTimestampMs: <current time>
        // - clientId: "test-producer"
        // - applicationName: "order-processor"
        // - schemaVersion: 1

        // Step 11: Published event asynchronously
        // (fire-and-forget, cannot verify in unit test)

        // Step 12: Returned modified record
        assertThat(modifiedRecord.topic()).isEqualTo("notifications");
        assertThat(modifiedRecord.key()).isEqualTo("notif-key");
        assertThat(modifiedRecord.value()).isEqualTo("notification-payload");

        // ===== VERIFY: Interceptor did NOT modify ThreadLocal or MDC =====
        TraceContext afterContext = TraceContext.current();
        assertThat(afterContext).isEqualTo(consumerContext);
        assertThat(afterContext.getTraceId()).isEqualTo("550e8400-e29b-41d4-a716-446655440000");
        assertThat(afterContext.getSpanId()).isEqualTo("6ba7b810-9dad-11d1-80b4-00c04fd430c8");

        TraceContext.TriggerMetadata afterTrigger = TraceContext.getTrigger();
        assertThat(afterTrigger).isEqualTo(trigger);

        // ===== VERIFY: Causal chain is correct =====
        // Chain: consumer span (6ba7b810...) -> producer span (new UUID)
        assertThat(parentSpanId).isEqualTo(consumerContext.getSpanId());
        assertThat(traceId).isEqualTo(consumerContext.getTraceId());
    }

    @Test
    void scenario2_multiHopChain_shouldPreserveCausality() {
        // Test a 3-hop causal chain: Service A -> Service B -> Service C

        // ===== HOP 1: Service A produces to "orders" (root span) =====
        String serviceA_traceId = "550e8400-e29b-41d4-a716-446655440000";
        String serviceA_spanId = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa";

        // ===== HOP 2: Service B consumes from "orders" and produces to "inventory" =====

        // Service B receives message with Service A's headers
        ConsumerRecord<String, String> serviceBConsumed = new ConsumerRecord<>(
                "orders", 0, 50L, "key1", "value1"
        );
        serviceBConsumed.headers().add(TraceContextPropagator.HEADER_TRACE_ID,
                serviceA_traceId.getBytes(StandardCharsets.UTF_8));
        serviceBConsumed.headers().add(TraceContextPropagator.HEADER_SPAN_ID,
                serviceA_spanId.getBytes(StandardCharsets.UTF_8));

        // Service B extracts context
        TraceContextPropagator.extractAndBind(serviceBConsumed);
        TraceContext.setTrigger("orders", 0, 50L, "serviceB-group");

        // Service B produces
        ProducerRecord<String, String> serviceBProduced = new ProducerRecord<>("inventory", "key2", "value2");
        ProducerRecord<String, String> serviceBModified = interceptor.onSend(serviceBProduced);

        String serviceB_traceId = new String(serviceBModified.headers()
                .lastHeader(TraceContextPropagator.HEADER_TRACE_ID).value(), StandardCharsets.UTF_8);
        String serviceB_spanId = new String(serviceBModified.headers()
                .lastHeader(TraceContextPropagator.HEADER_SPAN_ID).value(), StandardCharsets.UTF_8);
        String serviceB_parentSpanId = new String(serviceBModified.headers()
                .lastHeader(TraceContextPropagator.HEADER_PARENT_SPAN_ID).value(), StandardCharsets.UTF_8);

        // Verify Service B created child span
        assertThat(serviceB_traceId).isEqualTo(serviceA_traceId);  // Same trace
        assertThat(serviceB_spanId).isNotEqualTo(serviceA_spanId);  // New span
        assertThat(serviceB_parentSpanId).isEqualTo(serviceA_spanId);  // Links to Service A

        // ===== HOP 3: Service C consumes from "inventory" and produces to "notifications" =====

        // Service C receives message with Service B's headers
        TraceContext.clearCurrent();  // Simulate new consumer thread

        ConsumerRecord<String, String> serviceCConsumed = new ConsumerRecord<>(
                "inventory", 1, 75L, "key3", "value3"
        );
        serviceCConsumed.headers().add(TraceContextPropagator.HEADER_TRACE_ID,
                serviceB_traceId.getBytes(StandardCharsets.UTF_8));
        serviceCConsumed.headers().add(TraceContextPropagator.HEADER_SPAN_ID,
                serviceB_spanId.getBytes(StandardCharsets.UTF_8));
        serviceCConsumed.headers().add(TraceContextPropagator.HEADER_PARENT_SPAN_ID,
                serviceB_parentSpanId.getBytes(StandardCharsets.UTF_8));

        // Service C extracts context
        TraceContextPropagator.extractAndBind(serviceCConsumed);
        TraceContext.setTrigger("inventory", 1, 75L, "serviceC-group");

        // Service C produces
        ProducerRecord<String, String> serviceCProduced = new ProducerRecord<>("notifications", "key4", "value4");
        ProducerRecord<String, String> serviceCModified = interceptor.onSend(serviceCProduced);

        String serviceC_traceId = new String(serviceCModified.headers()
                .lastHeader(TraceContextPropagator.HEADER_TRACE_ID).value(), StandardCharsets.UTF_8);
        String serviceC_spanId = new String(serviceCModified.headers()
                .lastHeader(TraceContextPropagator.HEADER_SPAN_ID).value(), StandardCharsets.UTF_8);
        String serviceC_parentSpanId = new String(serviceCModified.headers()
                .lastHeader(TraceContextPropagator.HEADER_PARENT_SPAN_ID).value(), StandardCharsets.UTF_8);

        // Verify Service C created child span
        assertThat(serviceC_traceId).isEqualTo(serviceA_traceId);  // Same trace across all hops
        assertThat(serviceC_spanId).isNotEqualTo(serviceB_spanId);  // New span
        assertThat(serviceC_parentSpanId).isEqualTo(serviceB_spanId);  // Links to Service B

        // ===== VERIFY: Complete causal chain =====
        // Service A (root) -> Service B (child) -> Service C (grandchild)
        // All share same traceId, each has unique spanId, each links to parent

        assertThat(serviceA_traceId).isEqualTo(serviceB_traceId).isEqualTo(serviceC_traceId);
        assertThat(serviceA_spanId).isNotEqualTo(serviceB_spanId).isNotEqualTo(serviceC_spanId);
        assertThat(serviceB_parentSpanId).isEqualTo(serviceA_spanId);
        assertThat(serviceC_parentSpanId).isEqualTo(serviceB_spanId);
    }

    @Test
    void scenario2_fanOut_shouldCreateMultipleChildSpans() {
        // Test fan-out pattern: One consumer produces to multiple topics

        // ===== GIVEN: Consumer received message =====
        ConsumerRecord<String, String> consumed = new ConsumerRecord<>(
                "orders", 0, 100L, "order-key", "order-data"
        );
        consumed.headers().add(TraceContextPropagator.HEADER_TRACE_ID,
                "550e8400-e29b-41d4-a716-446655440000".getBytes(StandardCharsets.UTF_8));
        consumed.headers().add(TraceContextPropagator.HEADER_SPAN_ID,
                "6ba7b810-9dad-11d1-80b4-00c04fd430c8".getBytes(StandardCharsets.UTF_8));

        TraceContextPropagator.extractAndBind(consumed);
        TraceContext.setTrigger("orders", 0, 100L, "order-processor");

        // ===== WHEN: Consumer produces to 3 different topics (fan-out) =====
        ProducerRecord<String, String> notificationRecord = new ProducerRecord<>("notifications", "k1", "v1");
        ProducerRecord<String, String> auditRecord = new ProducerRecord<>("audit", "k2", "v2");
        ProducerRecord<String, String> analyticsRecord = new ProducerRecord<>("analytics", "k3", "v3");

        ProducerRecord<String, String> notificationModified = interceptor.onSend(notificationRecord);
        ProducerRecord<String, String> auditModified = interceptor.onSend(auditRecord);
        ProducerRecord<String, String> analyticsModified = interceptor.onSend(analyticsRecord);

        // ===== THEN: Each should have unique spanId but same traceId and parentSpanId =====
        String notificationSpanId = new String(notificationModified.headers()
                .lastHeader(TraceContextPropagator.HEADER_SPAN_ID).value(), StandardCharsets.UTF_8);
        String auditSpanId = new String(auditModified.headers()
                .lastHeader(TraceContextPropagator.HEADER_SPAN_ID).value(), StandardCharsets.UTF_8);
        String analyticsSpanId = new String(analyticsModified.headers()
                .lastHeader(TraceContextPropagator.HEADER_SPAN_ID).value(), StandardCharsets.UTF_8);

        // All spanIds are unique
        assertThat(notificationSpanId).isNotEqualTo(auditSpanId);
        assertThat(auditSpanId).isNotEqualTo(analyticsSpanId);
        assertThat(notificationSpanId).isNotEqualTo(analyticsSpanId);

        // All share same traceId
        String notificationTraceId = new String(notificationModified.headers()
                .lastHeader(TraceContextPropagator.HEADER_TRACE_ID).value(), StandardCharsets.UTF_8);
        String auditTraceId = new String(auditModified.headers()
                .lastHeader(TraceContextPropagator.HEADER_TRACE_ID).value(), StandardCharsets.UTF_8);
        String analyticsTraceId = new String(analyticsModified.headers()
                .lastHeader(TraceContextPropagator.HEADER_TRACE_ID).value(), StandardCharsets.UTF_8);

        assertThat(notificationTraceId).isEqualTo("550e8400-e29b-41d4-a716-446655440000");
        assertThat(auditTraceId).isEqualTo("550e8400-e29b-41d4-a716-446655440000");
        assertThat(analyticsTraceId).isEqualTo("550e8400-e29b-41d4-a716-446655440000");

        // All link to same parent
        String notificationParentSpanId = new String(notificationModified.headers()
                .lastHeader(TraceContextPropagator.HEADER_PARENT_SPAN_ID).value(), StandardCharsets.UTF_8);
        String auditParentSpanId = new String(auditModified.headers()
                .lastHeader(TraceContextPropagator.HEADER_PARENT_SPAN_ID).value(), StandardCharsets.UTF_8);
        String analyticsParentSpanId = new String(analyticsModified.headers()
                .lastHeader(TraceContextPropagator.HEADER_PARENT_SPAN_ID).value(), StandardCharsets.UTF_8);

        assertThat(notificationParentSpanId).isEqualTo("6ba7b810-9dad-11d1-80b4-00c04fd430c8");
        assertThat(auditParentSpanId).isEqualTo("6ba7b810-9dad-11d1-80b4-00c04fd430c8");
        assertThat(analyticsParentSpanId).isEqualTo("6ba7b810-9dad-11d1-80b4-00c04fd430c8");
    }
}
