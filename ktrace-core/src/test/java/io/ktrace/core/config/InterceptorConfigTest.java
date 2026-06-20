package io.ktrace.core.config;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class InterceptorConfigTest {

    @AfterEach
    void cleanup() {
        // Clean up system properties after each test
        System.clearProperty("ktrace.enabled");
        System.clearProperty("ktrace.trace-topic");
        System.clearProperty("ktrace.application-name");
        System.clearProperty("ktrace.async-queue-size");
        System.clearProperty("ktrace.close-timeout-ms");
    }

    @Test
    void fromProducerConfig_shouldUseDefaultValues() {
        // Clear any system properties that might interfere
        System.clearProperty("ktrace.enabled");
        System.clearProperty("ktrace.trace-topic");

        Map<String, Object> configs = new HashMap<>();
        configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        InterceptorConfig config = InterceptorConfig.fromProducerConfig(configs);

        // Note: These are defaults when nothing is configured
        // ktrace.enabled defaults to true
        // ktrace.trace-topic defaults to __ktrace
        assertThat(config.getTraceTopic()).isEqualTo("__ktrace");
        assertThat(config.getAsyncQueueSize()).isEqualTo(1000);
        assertThat(config.getCloseTimeoutMs()).isEqualTo(5000);
        assertThat(config.getApplicationName()).isNull();
        assertThat(config.getClientId()).isEqualTo("ktrace-producer-1");
    }

    @Test
    void fromProducerConfig_shouldUseProducerConfigMap() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        configs.put("ktrace.enabled", "false");
        configs.put("ktrace.trace-topic", "__custom");
        configs.put("ktrace.async-queue-size", "500");
        configs.put("ktrace.close-timeout-ms", "3000");
        configs.put("ktrace.application-name", "test-app");
        configs.put(ProducerConfig.CLIENT_ID_CONFIG, "my-client");

        InterceptorConfig config = InterceptorConfig.fromProducerConfig(configs);

        assertThat(config.isEnabled()).isFalse();
        assertThat(config.getTraceTopic()).isEqualTo("__custom");
        assertThat(config.getAsyncQueueSize()).isEqualTo(500);
        assertThat(config.getCloseTimeoutMs()).isEqualTo(3000);
        assertThat(config.getApplicationName()).isEqualTo("test-app");
        assertThat(config.getClientId()).isEqualTo("my-client");
    }

    @Test
    void fromProducerConfig_shouldFallbackToSystemProperty() {
        // Set system property
        System.setProperty("ktrace.enabled", "false");
        System.setProperty("ktrace.trace-topic", "__sysprop");
        System.setProperty("ktrace.application-name", "sysprop-app");

        Map<String, Object> configs = new HashMap<>();
        configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        // No ktrace.* in producer config

        InterceptorConfig config = InterceptorConfig.fromProducerConfig(configs);

        assertThat(config.isEnabled()).isFalse();
        assertThat(config.getTraceTopic()).isEqualTo("__sysprop");
        assertThat(config.getApplicationName()).isEqualTo("sysprop-app");
    }

    @Test
    void fromProducerConfig_producerConfigOverridesSystemProperty() {
        // Set system property
        System.setProperty("ktrace.enabled", "false");

        Map<String, Object> configs = new HashMap<>();
        configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        configs.put("ktrace.enabled", "true"); // Override system property

        InterceptorConfig config = InterceptorConfig.fromProducerConfig(configs);

        assertThat(config.isEnabled()).isTrue(); // Producer config wins
    }

    @Test
    void fromProducerConfig_clientIdDerivedFromApplicationName() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        configs.put("ktrace.application-name", "order-service");
        // No client.id

        InterceptorConfig config = InterceptorConfig.fromProducerConfig(configs);

        assertThat(config.getClientId()).isEqualTo("order-service~1");
    }

    @Test
    void fromProducerConfig_clientIdExplicitOverridesDefault() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        configs.put("ktrace.application-name", "order-service");
        configs.put(ProducerConfig.CLIENT_ID_CONFIG, "custom-client");

        InterceptorConfig config = InterceptorConfig.fromProducerConfig(configs);

        assertThat(config.getClientId()).isEqualTo("custom-client");
    }

    @Test
    void fromProducerConfig_shouldParseBooleanTypes() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        configs.put("ktrace.enabled", true); // Boolean type, not String

        InterceptorConfig config = InterceptorConfig.fromProducerConfig(configs);

        assertThat(config.isEnabled()).isTrue();
    }

    @Test
    void fromProducerConfig_shouldParseIntegerTypes() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        configs.put("ktrace.async-queue-size", 2000); // Integer type, not String

        InterceptorConfig config = InterceptorConfig.fromProducerConfig(configs);

        assertThat(config.getAsyncQueueSize()).isEqualTo(2000);
    }

    @Test
    void fromProducerConfig_invalidIntegerFallsBackToDefault() {
        Map<String, Object> configs = new HashMap<>();
        configs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        configs.put("ktrace.async-queue-size", "invalid"); // Invalid integer

        InterceptorConfig config = InterceptorConfig.fromProducerConfig(configs);

        assertThat(config.getAsyncQueueSize()).isEqualTo(1000); // Falls back to default
    }
}
