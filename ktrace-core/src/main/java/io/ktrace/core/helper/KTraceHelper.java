package io.ktrace.core.helper;

import io.ktrace.core.context.KTraceMDC;
import io.ktrace.core.context.TraceContext;
import io.ktrace.core.context.TraceContextPropagator;
import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * Helper for executing consumer code with automatic trace context and cleanup.
 * <p>
 * Ensures trace context is always cleaned up, even if user forgets or code throws exception.
 * Perfect for plain Java consumers without framework auto-wiring.
 * <p>
 * Usage:
 * <pre>
 * consumer.poll(Duration.ofSeconds(1)).forEach(record ->
 *     KTraceHelper.executeWithTrace(record, r -> {
 *         log.info("Processing order: {}", r.value());
 *         producer.send(newRecord);  // Headers auto-injected
 *         return null;
 *     })
 * );
 * </pre>
 */
public final class KTraceHelper {

    private KTraceHelper() {
    }

    /**
     * Executes code with automatic trace context binding and cleanup.
     * <p>
     * This method:
     * <ol>
     *   <li>Extracts trace context from record headers (or creates root if missing)</li>
     *   <li>Sets ThreadLocal and MDC</li>
     *   <li>Executes user code</li>
     *   <li>Cleans up ThreadLocal and MDC (even on exception)</li>
     * </ol>
     * <p>
     * User code never needs to call cleanup - it's guaranteed.
     *
     * @param <T>     the return type
     * @param record  the consumer record
     * @param handler the code to execute with trace context
     * @return the result from handler
     * @throws NullPointerException if record or handler is null
     */
    public static <T> T executeWithTrace(
            ConsumerRecord<?, ?> record,
            RecordHandler<T> handler
    ) throws Exception {
        if (record == null) {
            throw new NullPointerException("record must not be null");
        }
        if (handler == null) {
            throw new NullPointerException("handler must not be null");
        }

        try {
            // Setup: extract or create trace context, set ThreadLocal + MDC
            TraceContextPropagator.extractOrCreateAndBind(record);

            // Execute user code
            return handler.handle(record);
        } finally {
            // Cleanup: guaranteed to happen even on exception
            TraceContext.clearCurrent();
            KTraceMDC.clear();
        }
    }

    /**
     * Functional interface for consumer record handling with trace context.
     *
     * @param <T> the return type
     */
    @FunctionalInterface
    public interface RecordHandler<T> {
        /**
         * Handles a consumer record with trace context already bound.
         * <p>
         * Cleanup is automatic - do not call KTraceMDC.clear() or
         * TraceContext.clearCurrent() in this method.
         *
         * @param record the consumer record
         * @return the result
         * @throws Exception if processing fails
         */
        T handle(ConsumerRecord<?, ?> record) throws Exception;
    }
}
