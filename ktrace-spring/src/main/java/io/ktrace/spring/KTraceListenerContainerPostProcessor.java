package io.ktrace.spring;

import io.ktrace.core.context.TraceContext;
import io.ktrace.core.context.TraceContextPropagator;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.kafka.listener.AbstractMessageListenerContainer;
import org.springframework.kafka.listener.AcknowledgingMessageListener;
import org.springframework.kafka.listener.BatchMessageListener;
import org.springframework.kafka.listener.ConsumerAwareMessageListener;
import org.springframework.kafka.listener.MessageListener;

/**
 * BeanPostProcessor that automatically wraps Kafka listener containers
 * to bind ktrace context on each consumed record.
 * <p>
 * For single-record listeners ({@link MessageListener}, {@link AcknowledgingMessageListener},
 * {@link ConsumerAwareMessageListener}), trace context is extracted from record headers
 * and bound to the current thread before the listener is invoked. Context is always
 * cleared after invocation (in a finally block).
 * <p>
 * Batch listeners are detected but NOT wrapped — a log message instructs the user
 * to use {@link KTraceConsumerSupport} for batch processing.
 * <p>
 * This follows the same approach used by Spring Cloud Sleuth and Zipkin Brave
 * for instrumenting Kafka listeners.
 *
 * @see KTraceConsumerSupport for batch listener support
 * @see TraceContextPropagator#extractOrCreateAndBind(ConsumerRecord)
 */
public class KTraceListenerContainerPostProcessor implements BeanPostProcessor {

    private static final Logger log = LoggerFactory.getLogger(KTraceListenerContainerPostProcessor.class);

    private boolean loggedOnce = false;

    @Override
    public Object postProcessAfterInitialization(Object bean, String beanName) throws BeansException {
        if (!(bean instanceof AbstractMessageListenerContainer)) {
            return bean;
        }
        AbstractMessageListenerContainer<?, ?> container = (AbstractMessageListenerContainer<?, ?>) bean;

        if (!loggedOnce) {
            log.info("ktrace consumer auto-binding enabled");
            loggedOnce = true;
        }

        try {
            Object listener = container.getContainerProperties().getMessageListener();

            if (listener == null) {
                return bean;
            }

            if (isBatchListener(listener)) {
                log.info(
                    "ktrace: batch listener detected on container '{}'. " +
                    "Use KTraceConsumerSupport.processBatch() for per-record trace context binding.",
                    beanName
                );
                return bean;
            }

            Object wrapped = wrapListener(listener);
            if (wrapped != null) {
                container.getContainerProperties().setMessageListener(wrapped);
                log.debug("ktrace: wrapped listener on container '{}'", beanName);
            }

        } catch (Exception e) {
            log.error("ktrace: failed to wrap listener on container '{}', continuing without tracing", beanName, e);
        }

        return bean;
    }

    private boolean isBatchListener(Object listener) {
        return listener instanceof BatchMessageListener;
    }

    @SuppressWarnings("unchecked")
    private Object wrapListener(Object listener) {
        if (listener instanceof ConsumerAwareMessageListener) {
            ConsumerAwareMessageListener<Object, Object> original =
                (ConsumerAwareMessageListener<Object, Object>) listener;
            return (ConsumerAwareMessageListener<Object, Object>) (record, consumer) -> {
                bindContext(record);
                try {
                    original.onMessage(record, consumer);
                } finally {
                    clearContext();
                }
            };
        }

        if (listener instanceof AcknowledgingMessageListener) {
            AcknowledgingMessageListener<Object, Object> original =
                (AcknowledgingMessageListener<Object, Object>) listener;
            return (AcknowledgingMessageListener<Object, Object>) (record, acknowledgment) -> {
                bindContext(record);
                try {
                    original.onMessage(record, acknowledgment);
                } finally {
                    clearContext();
                }
            };
        }

        if (listener instanceof MessageListener) {
            MessageListener<Object, Object> original =
                (MessageListener<Object, Object>) listener;
            return (MessageListener<Object, Object>) record -> {
                bindContext(record);
                try {
                    original.onMessage(record);
                } finally {
                    clearContext();
                }
            };
        }

        log.debug("ktrace: unknown listener type '{}', skipping wrapping", listener.getClass().getName());
        return null;
    }

    private void bindContext(ConsumerRecord<?, ?> record) {
        try {
            TraceContextPropagator.extractOrCreateAndBind(record);
        } catch (Exception e) {
            log.error("ktrace: failed to bind trace context, continuing without tracing", e);
        }
    }

    private void clearContext() {
        try {
            TraceContext.clearCurrent();
        } catch (Exception e) {
            log.error("ktrace: failed to clear trace context", e);
        }
    }
}
