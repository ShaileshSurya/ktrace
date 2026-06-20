package io.ktrace.core.config;

import org.apache.kafka.clients.producer.ProducerConfig;

import java.io.InputStream;
import java.util.Map;
import java.util.Properties;

/**
 * Configuration for KTraceProducerInterceptor.
 * <p>
 * Parses ktrace.* properties with fallback priority:
 * <ol>
 *   <li>Producer config Map (programmatic)</li>
 *   <li>System property (-Dktrace.enabled=true)</li>
 *   <li>Environment variable (KTRACE_ENABLED=true)</li>
 *   <li>application.properties (classpath)</li>
 *   <li>Defaults</li>
 * </ol>
 */
public final class InterceptorConfig {

    private final boolean enabled;
    private final String traceTopic;
    private final int asyncQueueSize;
    private final int closeTimeoutMs;
    private final String applicationName;
    private final String clientId;
    private final int publisherRetries;

    private InterceptorConfig(boolean enabled, String traceTopic,
                              int asyncQueueSize, int closeTimeoutMs,
                              String applicationName, String clientId,
                              int publisherRetries) {
        this.enabled = enabled;
        this.traceTopic = traceTopic;
        this.asyncQueueSize = asyncQueueSize;
        this.closeTimeoutMs = closeTimeoutMs;
        this.applicationName = applicationName;
        this.clientId = clientId;
        this.publisherRetries = publisherRetries;
    }

    /**
     * Parses ktrace configuration from Kafka producer config Map.
     * Falls back to KTraceProperties (system props, env vars, application.properties).
     *
     * @param configs producer configuration map
     * @return parsed InterceptorConfig
     */
    public static InterceptorConfig fromProducerConfig(Map<String, ?> configs) {
        boolean enabled = resolveBoolean(configs, "ktrace.enabled", true);
        String traceTopic = resolveString(configs, "ktrace.trace-topic", "__ktrace");
        int queueSize = resolveInt(configs, "ktrace.async-queue-size", 1000);
        int timeout = resolveInt(configs, "ktrace.close-timeout-ms", 5000);
        String appName = resolveString(configs, "ktrace.application-name", null);
        int retries = resolveInt(configs, "ktrace.trace-topic-producer-retries", 0);

        String clientId = resolveString(configs, ProducerConfig.CLIENT_ID_CONFIG, null);
        if (clientId == null) {
            clientId = (appName != null) ? appName + "~1" : "ktrace-producer-1";
        }

        return new InterceptorConfig(enabled, traceTopic, queueSize, timeout, appName, clientId, retries);
    }

    /**
     * Resolves a boolean property with priority:
     * 1. Producer config Map (highest)
     * 2. KTraceProperties (sysprop → env → file)
     * 3. Default value
     */
    private static boolean resolveBoolean(Map<String, ?> configs, String key, boolean defaultValue) {
        // Priority 1: Producer config Map
        Object value = configs.get(key);
        if (value != null) {
            if (value instanceof Boolean) {
                return (Boolean) value;
            }
            return Boolean.parseBoolean(value.toString());
        }

        // Priority 2: KTraceProperties (handles sysprop, env, file)
        return KTraceProperties.getBoolean(key, defaultValue);
    }

    /**
     * Resolves a string property with priority.
     */
    private static String resolveString(Map<String, ?> configs, String key, String defaultValue) {
        // Priority 1: Producer config Map
        Object value = configs.get(key);
        if (value != null) {
            return value.toString();
        }

        // Priority 2: KTraceProperties
        return KTraceProperties.get(key, defaultValue);
    }

    /**
     * Resolves an int property with priority.
     */
    private static int resolveInt(Map<String, ?> configs, String key, int defaultValue) {
        // Priority 1: Producer config Map
        Object value = configs.get(key);
        if (value != null) {
            if (value instanceof Integer) {
                return (Integer) value;
            }
            try {
                return Integer.parseInt(value.toString());
            } catch (NumberFormatException e) {
                // Fall through to KTraceProperties
            }
        }

        // Priority 2: KTraceProperties
        return KTraceProperties.getInt(key, defaultValue);
    }

    public boolean isEnabled() {
        return enabled;
    }

    public String getTraceTopic() {
        return traceTopic;
    }

    public int getAsyncQueueSize() {
        return asyncQueueSize;
    }

    public int getCloseTimeoutMs() {
        return closeTimeoutMs;
    }

    public String getApplicationName() {
        return applicationName;
    }

    public String getClientId() {
        return clientId;
    }

    public int getPublisherRetries() {
        return publisherRetries;
    }

    private static boolean parseBoolean(Map<String, ?> configs, String key, boolean defaultValue) {
        Object value = configs.get(key);
        if (value == null) {
            return defaultValue;
        }
        if (value instanceof Boolean) {
            return (Boolean) value;
        }
        return Boolean.parseBoolean(value.toString());
    }

    private static String parseString(Map<String, ?> configs, String key, String defaultValue) {
        Object value = configs.get(key);
        return (value != null) ? value.toString() : defaultValue;
    }

    private static int parseInt(Map<String, ?> configs, String key, int defaultValue) {
        Object value = configs.get(key);
        if (value == null) {
            return defaultValue;
        }
        if (value instanceof Integer) {
            return (Integer) value;
        }
        try {
            return Integer.parseInt(value.toString());
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }
}
