package io.ktrace.spring;

import io.ktrace.core.context.TraceContext;
import io.ktrace.core.context.TraceContextPropagator;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.function.Consumer;

/**
 * Helper utility for manual trace context binding in Kafka listeners.
 * <p>
 * Provides convenience methods for batch listeners where automatic context
 * binding is not possible (multiple records with different trace contexts).
 * <p>
 * Example usage for batch listeners:
 * <pre>
 * &#64;KafkaListener(topics = "orders", batch = "true")
 * public void onOrders(List&lt;ConsumerRecord&lt;String, String&gt;&gt; records) {
 *     KTraceConsumerSupport.processBatch(records, record -&gt; {
 *         // Each record processed with its own trace context
 *         kafkaTemplate.send("notifications", "notif-1", "{}");
 *     });
 * }
 * </pre>
 * <p>
 * <b>Note:</b> Single-record listeners get automatic context binding via
 * {@link KTraceListenerContainerPostProcessor}. This utility is only needed
 * for batch listeners.
 * <p>
 * <b>Error Handling:</b> This utility uses graceful degradation. If trace context
 * binding fails, the error is logged but processing continues. ktrace never throws
 * exceptions that would break your application logic.
 *
 * @see TraceContextPropagator
 * @see KTraceListenerContainerPostProcessor
 */
public final class KTraceConsumerSupport {

    private static final Logger log = LoggerFactory.getLogger(KTraceConsumerSupport.class);

    private KTraceConsumerSupport() {
        // Prevent instantiation
    }

    /**
     * Process a batch of ConsumerRecords with per-record trace context binding.
     * <p>
     * For each record in the batch:
     * <ol>
     *   <li>Extracts trace context from record headers (or creates root context)</li>
     *   <li>Binds context to ThreadLocal and MDC</li>
     *   <li>Invokes the processor with the record</li>
     *   <li>Clears context from ThreadLocal and MDC</li>
     * </ol>
     * <p>
     * Context is always cleared in a finally block, even if processor throws an exception.
     * Exceptions from the processor propagate to the caller.
     * <p>
     * If trace context binding fails, the error is logged and processing continues
     * without tracing (graceful degradation).
     *
     * @param records the batch of records to process
     * @param processor the processing logic for each record
     * @param <K> record key type
     * @param <V> record value type
     */
    public static <K, V> void processBatch(
            List<ConsumerRecord<K, V>> records,
            Consumer<ConsumerRecord<K, V>> processor) {

        // Graceful null handling
        if (records == null) {
            log.warn("ktrace: processBatch called with null records, skipping tracing");
            return;
        }
        if (processor == null) {
            log.warn("ktrace: processBatch called with null processor, skipping");
            return;
        }

        for (ConsumerRecord<K, V> record : records) {
            // Try to bind context, but don't fail if it doesn't work
            try {
                TraceContextPropagator.extractOrCreateAndBind(record);
            } catch (Exception e) {
                log.error("ktrace: failed to bind trace context for record, continuing without tracing", e);
            }

            try {
                processor.accept(record);  // User's business logic - let exceptions propagate
            } finally {
                // Always cleanup, even if processor or binding failed
                try {
                    TraceContext.clearCurrent();
                } catch (Exception e) {
                    log.error("ktrace: failed to clear trace context", e);
                }
            }
        }
    }

    /**
     * Process a single ConsumerRecord with trace context binding.
     * <p>
     * This is a convenience method for single-record processing when automatic
     * binding is not available. In most cases, single-record listeners get
     * automatic context binding and don't need this method.
     * <p>
     * Use this when:
     * <ul>
     *   <li>Processing records outside of &#64;KafkaListener (e.g., manual Consumer.poll())</li>
     *   <li>Custom listener implementations</li>
     *   <li>Testing scenarios</li>
     * </ul>
     * <p>
     * If trace context binding fails, the error is logged and processing continues
     * without tracing (graceful degradation).
     *
     * @param record the record to process
     * @param processor the processing logic
     * @param <K> record key type
     * @param <V> record value type
     */
    public static <K, V> void processRecord(
            ConsumerRecord<K, V> record,
            Consumer<ConsumerRecord<K, V>> processor) {

        // Graceful null handling
        if (record == null) {
            log.warn("ktrace: processRecord called with null record, skipping tracing");
            return;
        }
        if (processor == null) {
            log.warn("ktrace: processRecord called with null processor, skipping");
            return;
        }

        // Try to bind context, but don't fail if it doesn't work
        try {
            TraceContextPropagator.extractOrCreateAndBind(record);
        } catch (Exception e) {
            log.error("ktrace: failed to bind trace context, continuing without tracing", e);
        }

        try {
            processor.accept(record);  // User's business logic - let exceptions propagate
        } finally {
            // Always cleanup
            try {
                TraceContext.clearCurrent();
            } catch (Exception e) {
                log.error("ktrace: failed to clear trace context", e);
            }
        }
    }

    /**
     * Process a single ConsumerRecord with trace context binding, using Runnable.
     * <p>
     * Similar to {@link #processRecord(ConsumerRecord, Consumer)} but for cases
     * where you don't need the record parameter in your processing logic.
     * <p>
     * Example:
     * <pre>
     * KTraceConsumerSupport.withTracing(record, () -&gt; {
     *     // Processing logic without needing the record parameter
     *     kafkaTemplate.send("topic", "value");
     * });
     * </pre>
     * <p>
     * If trace context binding fails, the error is logged and processing continues
     * without tracing (graceful degradation).
     *
     * @param record the record to extract context from
     * @param processor the processing logic
     */
    public static void withTracing(ConsumerRecord<?, ?> record, Runnable processor) {
        // Graceful null handling
        if (record == null) {
            log.warn("ktrace: withTracing called with null record, skipping tracing");
            return;
        }
        if (processor == null) {
            log.warn("ktrace: withTracing called with null processor, skipping");
            return;
        }

        // Try to bind context, but don't fail if it doesn't work
        try {
            TraceContextPropagator.extractOrCreateAndBind(record);
        } catch (Exception e) {
            log.error("ktrace: failed to bind trace context, continuing without tracing", e);
        }

        try {
            processor.run();  // User's business logic - let exceptions propagate
        } finally {
            // Always cleanup
            try {
                TraceContext.clearCurrent();
            } catch (Exception e) {
                log.error("ktrace: failed to clear trace context", e);
            }
        }
    }
}
