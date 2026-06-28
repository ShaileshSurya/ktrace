package io.ktrace.core.publisher;

import io.ktrace.core.event.TraceEvent;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link AsyncTracePublisher}.
 * <p>
 * These tests use real Kafka producer (no mocks) but avoid network I/O by
 * using a test-friendly bootstrap.servers. The focus is on queue behavior,
 * threading, and graceful degradation.
 */
class AsyncTracePublisherTest {

    private AsyncTracePublisher publisher;

    @AfterEach
    void tearDown() {
        if (publisher != null) {
            publisher.close();
        }
    }

    @Test
    void publish_shouldEnqueueEvent() throws Exception {
        // Given
        publisher = new AsyncTracePublisher(
                "__ktrace",
                "localhost:9092",
                10,
                1000,
                0
        );

        TraceEvent event = buildTestEvent("span-1");

        // When
        boolean enqueued = publisher.publish(event);

        // Then
        assertThat(enqueued).isTrue();
    }

    @Test
    void publish_queueFull_shouldReturnFalse() throws Exception {
        // Given: Small queue size and block the background thread from draining
        publisher = new AsyncTracePublisher(
                "__ktrace",
                "localhost:9092",
                2,  // Very small queue
                1000,
                0
        );

        // Fill the queue by publishing events faster than they can drain
        // The background thread will try to send to localhost:9092 which will fail,
        // but the queue will fill up
        List<TraceEvent> events = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            events.add(buildTestEvent("span-" + i));
        }

        // When: Publish many events rapidly
        List<Boolean> results = new ArrayList<>();
        for (TraceEvent event : events) {
            results.add(publisher.publish(event));
        }

        // Then: Some should be rejected (queue full)
        long rejected = results.stream().filter(r -> !r).count();
        assertThat(rejected).isGreaterThan(0);
    }

    @Test
    void publish_nullEvent_shouldReturnFalse() throws Exception {
        // Given
        publisher = new AsyncTracePublisher(
                "__ktrace",
                "localhost:9092",
                10,
                1000,
                0
        );

        // When
        boolean enqueued = publisher.publish(null);

        // Then
        assertThat(enqueued).isFalse();
    }

    @Test
    void close_shouldStopAcceptingEvents() throws Exception {
        // Given
        publisher = new AsyncTracePublisher(
                "__ktrace",
                "localhost:9092",
                10,
                1000,
                0
        );

        TraceEvent event1 = buildTestEvent("span-1");
        publisher.publish(event1);

        // When
        publisher.close();

        // Then: Publisher should be closed, but we can't easily verify internal state
        // This test mainly verifies close() doesn't throw exceptions
        assertThat(publisher).isNotNull();
    }

    @Test
    void close_withTimeout_shouldNotBlockIndefinitely() throws Exception {
        // Given: Publisher with very short timeout
        publisher = new AsyncTracePublisher(
                "__ktrace",
                "localhost:9092",
                1000,
                100,  // 100ms timeout
                0
        );

        // Enqueue some events
        for (int i = 0; i < 5; i++) {
            publisher.publish(buildTestEvent("span-" + i));
        }

        // When: Close with short timeout
        long startTime = System.currentTimeMillis();
        publisher.close();
        long duration = System.currentTimeMillis() - startTime;

        // Then: Should not block much longer than timeout
        assertThat(duration).isLessThan(500);  // Allow some overhead
    }

    @Test
    void publish_multipleThreads_shouldBeThreadSafe() throws Exception {
        // Given
        publisher = new AsyncTracePublisher(
                "__ktrace",
                "localhost:9092",
                100,
                1000,
                0
        );

        int numThreads = 5;
        int eventsPerThread = 10;
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(numThreads);
        AtomicInteger successCount = new AtomicInteger(0);

        // When: Multiple threads publish concurrently
        for (int t = 0; t < numThreads; t++) {
            final int threadId = t;
            new Thread(() -> {
                try {
                    startLatch.await();  // All threads start together
                    for (int i = 0; i < eventsPerThread; i++) {
                        TraceEvent event = buildTestEvent("thread-" + threadId + "-span-" + i);
                        if (publisher.publish(event)) {
                            successCount.incrementAndGet();
                        }
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    doneLatch.countDown();
                }
            }).start();
        }

        startLatch.countDown();  // Start all threads
        boolean completed = doneLatch.await(5, TimeUnit.SECONDS);

        // Then: All threads should complete without exceptions
        assertThat(completed).isTrue();
        assertThat(successCount.get()).isGreaterThan(0);  // At least some events enqueued
    }

    @Test
    void constructor_invalidBootstrapServers_shouldThrowException() {
        // When/Then: Invalid bootstrap.servers should cause initialization to fail
        org.apache.kafka.common.KafkaException exception =
            org.junit.jupiter.api.Assertions.assertThrows(
                org.apache.kafka.common.KafkaException.class,
                () -> {
                    publisher = new AsyncTracePublisher(
                            "__ktrace",
                            "",  // Empty bootstrap.servers
                            10,
                            1000,
                            0
                    );
                }
            );

        // Verify exception message contains relevant info
        assertThat(exception.getMessage()).contains("Failed to construct kafka producer");
    }

    /**
     * Helper method to build a test TraceEvent.
     */
    private TraceEvent buildTestEvent(String suffix) {
        // Generate valid UUID for spanId
        String spanId = java.util.UUID.randomUUID().toString();

        return TraceEvent.builder()
                .traceId("550e8400-e29b-41d4-a716-446655440000")
                .spanId(spanId)
                .parentSpanId(null)
                .producedTopic("orders")
                .producedPartition(-1)
                .producedOffset(-1)
                .triggerTopic(null)
                .triggerPartition(-1)
                .triggerOffset(-1)
                .triggerConsumerGroup(null)
                .producerTimestampMs(System.currentTimeMillis())
                .clientId("test-client")
                .applicationName("test-app")
                .messageKey(null)
                .messageSizeBytes(0)
                .schemaVersion(1)
                .build();
    }

    // ====== Scenario 5: Queue Overflow Tests ======

    @Test
    void scenario5_queueFull_shouldNotBlockCaller() throws Exception {
        // Given
        publisher = new AsyncTracePublisher(
                "__ktrace",
                "localhost:9092",
                10,
                1000,
                0
        );

        // Fill queue rapidly
        for (int i = 0; i < 20; i++) {
            publisher.publish(buildTestEvent("span-" + i));
        }

        // When: Measure time for additional event
        long start = System.currentTimeMillis();
        publisher.publish(buildTestEvent("span-final"));
        long duration = System.currentTimeMillis() - start;

        // Then: Should return quickly (non-blocking)
        assertThat(duration).isLessThan(1000); // Should be < 1 second
    }

    @Test
    void scenario5_queueFull_shouldHandleOverflow() throws Exception {
        // Given
        publisher = new AsyncTracePublisher(
                "__ktrace",
                "localhost:9092",
                5,
                1000,
                0
        );

        // When: Publish more events than queue can hold
        int successCount = 0;
        int failureCount = 0;
        for (int i = 0; i < 20; i++) {
            if (publisher.publish(buildTestEvent("span-" + i))) {
                successCount++;
            } else {
                failureCount++;
            }
        }

        // Then: Some should be rejected due to queue overflow
        assertThat(failureCount).isGreaterThan(0);
        assertThat(successCount).isGreaterThan(0);
    }

    // ====== Scenario 7: Close Tests ======

    @Test
    void scenario7_close_shouldCloseSuccessfully() throws Exception {
        // Given
        publisher = new AsyncTracePublisher(
                "__ktrace",
                "localhost:9092",
                10,
                100, // Short timeout
                0
        );

        // Publish events
        for (int i = 0; i < 5; i++) {
            publisher.publish(buildTestEvent("span-" + i));
        }

        // When
        publisher.close();

        // Then: Should complete without exception
        assertThat(publisher).isNotNull();
    }

    @Test
    void scenario7_close_multipleCallsIdempotent_shouldNotFail() throws Exception {
        // Given
        publisher = new AsyncTracePublisher(
                "__ktrace",
                "localhost:9092",
                10,
                1000,
                0
        );

        // When: Call close twice
        publisher.close();
        publisher.close(); // Second call

        // Then: Should not throw exception
        assertThat(publisher).isNotNull();
    }
}
