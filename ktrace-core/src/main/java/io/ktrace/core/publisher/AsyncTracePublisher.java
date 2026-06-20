package io.ktrace.core.publisher;

import io.ktrace.core.event.TraceEvent;
import io.ktrace.core.event.TraceEventSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Asynchronous publisher for {@link TraceEvent} objects.
 * <p>
 * This class maintains an in-memory bounded queue and a background thread that
 * drains the queue and publishes events to the configured Kafka trace topic
 * (typically {@code __ktrace}).
 * <p>
 * Publishing is fire-and-forget with configurable retries. If the queue is full,
 * new events are dropped (not oldest events). This is best-effort tracing and
 * should not block the application's critical path.
 * <p>
 * Thread safety: {@link #publish(TraceEvent)} is thread-safe and can be called
 * from multiple threads concurrently. The internal queue is bounded and uses
 * non-blocking operations.
 *
 * @see TraceEvent
 * @see io.ktrace.core.interceptor.KTraceProducerInterceptor
 */
public final class AsyncTracePublisher {

    private static final Logger log = LoggerFactory.getLogger(AsyncTracePublisher.class);

    private final BlockingQueue<TraceEvent> queue;
    private final KafkaProducer<String, TraceEvent> traceProducer;
    private final Thread publisherThread;
    private final String traceTopic;
    private final int closeTimeoutMs;
    private volatile boolean running;

    /**
     * Creates a new async trace publisher.
     * <p>
     * This constructor initializes an internal Kafka producer and starts a
     * background daemon thread for publishing. If the internal producer fails
     * to initialize (e.g., invalid bootstrap.servers), a KafkaException is thrown.
     *
     * @param traceTopic target Kafka topic for trace events (e.g., "__ktrace")
     * @param bootstrapServers Kafka cluster address (inherited from application producer)
     * @param queueSize bounded queue capacity (default: 1000)
     * @param closeTimeoutMs max time to wait for queue drain on close (default: 5000ms)
     * @param publisherRetries retry count for internal producer (default: 0, fire-and-forget)
     * @throws org.apache.kafka.common.KafkaException if internal producer initialization fails
     */
    public AsyncTracePublisher(String traceTopic,
                               String bootstrapServers,
                               int queueSize,
                               int closeTimeoutMs,
                               int publisherRetries) {
        this.traceTopic = traceTopic;
        this.closeTimeoutMs = closeTimeoutMs;
        this.queue = new ArrayBlockingQueue<>(queueSize);
        this.running = true;

        // Configure internal Kafka producer
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, TraceEventSerializer.class.getName());
        props.put(ProducerConfig.LINGER_MS_CONFIG, 10);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        props.put(ProducerConfig.ACKS_CONFIG, "1");
        props.put(ProducerConfig.RETRIES_CONFIG, publisherRetries);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "ktrace-publisher");

        // May throw KafkaException if bootstrap.servers unreachable
        this.traceProducer = new KafkaProducer<>(props);

        // Start background daemon thread
        this.publisherThread = new Thread(this::runPublishLoop, "ktrace-publisher-thread");
        this.publisherThread.setDaemon(true);
        this.publisherThread.start();
    }

    /**
     * Enqueues a trace event for asynchronous publishing.
     * <p>
     * This method is non-blocking and thread-safe. If the queue is full,
     * the event is dropped and a warning is logged.
     *
     * @param event the trace event to publish (must not be null)
     * @return true if enqueued successfully, false if queue is full (event dropped)
     */
    public boolean publish(TraceEvent event) {
        if (event == null) {
            return false;
        }

        boolean enqueued = queue.offer(event);
        if (!enqueued) {
            log.warn("ktrace queue full, dropping event for spanId={}", event.getSpanId());
        }
        return enqueued;
    }

    /**
     * Stops the publisher and attempts to drain the queue.
     * <p>
     * This method blocks the calling thread for up to {@code closeTimeoutMs}
     * milliseconds, waiting for the background thread to publish remaining events.
     * If the timeout expires, remaining events are dropped and a warning is logged.
     * <p>
     * This is intended for application shutdown only (not per-request).
     */
    public void close() {
        running = false;

        try {
            // Wait for background thread to finish draining
            publisherThread.join(closeTimeoutMs);

            if (publisherThread.isAlive()) {
                int remaining = queue.size();
                if (remaining > 0) {
                    log.warn("ktrace publisher did not drain queue within {}ms, {} events dropped",
                            closeTimeoutMs, remaining);
                }
                publisherThread.interrupt();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            traceProducer.close(Duration.ofSeconds(5));
        }
    }

    /**
     * Background thread loop that drains the queue and publishes to Kafka.
     * <p>
     * Runs until {@code running} flag is set to false. Uses fire-and-forget
     * publishing (no callbacks). Errors are logged but don't crash the thread.
     */
    private void runPublishLoop() {
        while (running) {
            try {
                TraceEvent event = queue.poll(100, TimeUnit.MILLISECONDS);
                if (event != null) {
                    // Sticky partitioning: key = null
                    ProducerRecord<String, TraceEvent> record =
                            new ProducerRecord<>(traceTopic, null, event);
                    traceProducer.send(record);  // Fire-and-forget, no callback
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                // Serialization, network, or other errors - log and continue
                log.error("Failed to publish TraceEvent, dropping event", e);
            }
        }
    }
}
