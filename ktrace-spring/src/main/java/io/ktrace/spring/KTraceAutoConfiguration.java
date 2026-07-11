package io.ktrace.spring;

import io.ktrace.core.interceptor.KTraceProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.boot.autoconfigure.kafka.DefaultKafkaProducerFactoryCustomizer;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Spring Boot auto-configuration for ktrace.
 * <p>
 * Automatically configures ktrace tracing for all Kafka producers when:
 * <ul>
 *   <li>Spring Kafka is on the classpath</li>
 *   <li>{@code ktrace.enabled=true} is set</li>
 * </ul>
 * <p>
 * Registers {@link KTraceProducerInterceptor} with all {@link org.springframework.kafka.core.ProducerFactory}
 * beans, enabling automatic causality tracing for all Kafka produces.
 *
 * @see KTraceProperties
 * @see KTraceConfigurer
 * @see KTraceDefaults
 */
@Configuration
@ConditionalOnClass(KafkaTemplate.class)
@ConditionalOnProperty(
    name = "ktrace.enabled",
    havingValue = "true",
    matchIfMissing = false
)
@EnableConfigurationProperties(KTraceProperties.class)
public class KTraceAutoConfiguration {

    private static final Logger log = LoggerFactory.getLogger(KTraceAutoConfiguration.class);

    @Bean
    public KTraceListenerContainerPostProcessor ktraceListenerContainerPostProcessor() {
        return new KTraceListenerContainerPostProcessor();
    }

    /**
     * Customizes all ProducerFactory beans to include ktrace interceptor.
     * <p>
     * Resolves configuration from multiple sources with priority:
     * <ol>
     *   <li>Explicit ktrace.* properties (highest priority)</li>
     *   <li>Programmatic {@link KTraceConfigurer} bean</li>
     *   <li>Spring Boot equivalents (e.g., spring.kafka.bootstrap-servers)</li>
     *   <li>Default values (lowest priority)</li>
     * </ol>
     *
     * @param ktraceProps ktrace configuration properties
     * @param springKafkaProps Spring Kafka configuration properties
     * @param environment Spring environment for resolving properties
     * @param configurer optional programmatic configuration
     * @return producer factory customizer that registers ktrace interceptor
     */
    @Bean
    public DefaultKafkaProducerFactoryCustomizer ktraceProducerCustomizer(
            KTraceProperties ktraceProps,
            KafkaProperties springKafkaProps,
            Environment environment,
            @Autowired(required = false) KTraceConfigurer configurer) {

        return producerFactory -> {
            // Resolve all configuration properties
            String bootstrapServers = resolveBootstrapServers(ktraceProps, configurer, springKafkaProps);
            String applicationName = resolveApplicationName(ktraceProps, configurer, environment);
            String traceTopic = resolveTraceTopic(ktraceProps, configurer);
            int asyncQueueSize = resolveAsyncQueueSize(ktraceProps, configurer);
            int closeTimeoutMs = resolveCloseTimeoutMs(ktraceProps, configurer);
            int retries = resolveTraceTopicProducerRetries(ktraceProps, configurer);

            // Validate required fields - graceful degradation if missing
            if (bootstrapServers == null) {
                log.error(
                    "ktrace configuration incomplete: bootstrap.servers not configured. " +
                    "Tracing disabled. Set one of:\n" +
                    "  - ktrace.bootstrap-servers in application.yml\n" +
                    "  - spring.kafka.bootstrap-servers in application.yml\n" +
                    "  - KTraceConfigurer.bootstrapServers() programmatically\n" +
                    "  - System property: -Dktrace.bootstrap-servers=...\n" +
                    "  - Environment variable: KTRACE_BOOTSTRAP_SERVERS=..."
                );
                return;
            }

            if (applicationName == null) {
                log.error(
                    "ktrace configuration incomplete: application-name not configured. " +
                    "Tracing disabled. Set one of:\n" +
                    "  - ktrace.application-name in application.yml\n" +
                    "  - spring.application.name in application.yml\n" +
                    "  - KTraceConfigurer.applicationName() programmatically\n" +
                    "  - System property: -Dktrace.application-name=...\n" +
                    "  - Environment variable: KTRACE_APPLICATION_NAME=..."
                );
                return;
            }

            // Build configuration map for interceptor
            Map<String, Object> config = new HashMap<>();

            // Register ktrace interceptor
            config.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG,
                       KTraceProducerInterceptor.class.getName());

            // Pass bootstrap.servers (required by interceptor)
            config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

            // Pass ktrace configuration (exact keys that interceptor expects)
            config.put("ktrace.enabled", true);
            config.put("ktrace.application-name", applicationName);
            config.put("ktrace.trace-topic", traceTopic);
            config.put("ktrace.async-queue-size", asyncQueueSize);
            config.put("ktrace.close-timeout-ms", closeTimeoutMs);
            config.put("ktrace.trace-topic-producer-retries", retries);

            // Update producer factory with ktrace configuration
            producerFactory.updateConfigs(config);

            log.info("ktrace auto-configuration successful: applicationName={}, traceTopic={}",
                     applicationName, traceTopic);
        };
    }

    /**
     * Resolves bootstrap.servers with priority chain.
     */
    private String resolveBootstrapServers(
            KTraceProperties props,
            KTraceConfigurer configurer,
            KafkaProperties springKafka) {

        // Priority 1: ktrace.bootstrap-servers
        if (props.getBootstrapServers() != null) {
            log.debug("ktrace bootstrap.servers resolved from: ktrace.bootstrap-servers");
            return props.getBootstrapServers();
        }

        // Priority 2: Programmatic KTraceConfigurer
        if (configurer != null && configurer.getBootstrapServers() != null) {
            log.debug("ktrace bootstrap.servers resolved from: KTraceConfigurer");
            return configurer.getBootstrapServers();
        }

        // Priority 3: spring.kafka.bootstrap-servers
        List<String> servers = springKafka.getBootstrapServers();
        if (servers != null && !servers.isEmpty()) {
            log.debug("ktrace bootstrap.servers resolved from: spring.kafka.bootstrap-servers");
            return String.join(",", servers);
        }

        return null;
    }

    /**
     * Resolves application-name with priority chain.
     */
    private String resolveApplicationName(
            KTraceProperties props,
            KTraceConfigurer configurer,
            Environment environment) {

        // Priority 1: ktrace.application-name
        if (props.getApplicationName() != null) {
            log.debug("ktrace application-name resolved from: ktrace.application-name");
            return props.getApplicationName();
        }

        // Priority 2: Programmatic KTraceConfigurer
        if (configurer != null && configurer.getApplicationName() != null) {
            log.debug("ktrace application-name resolved from: KTraceConfigurer");
            return configurer.getApplicationName();
        }

        // Priority 3: spring.application.name
        String springAppName = environment.getProperty("spring.application.name");
        if (springAppName != null) {
            log.debug("ktrace application-name resolved from: spring.application.name");
            return springAppName;
        }

        return null;
    }

    /**
     * Resolves trace-topic with priority chain.
     */
    private String resolveTraceTopic(
            KTraceProperties props,
            KTraceConfigurer configurer) {

        // Priority 1: ktrace.trace-topic
        if (props.getTraceTopic() != null && !props.getTraceTopic().equals(KTraceDefaults.TRACE_TOPIC)) {
            log.debug("ktrace trace-topic resolved from: ktrace.trace-topic");
            return props.getTraceTopic();
        }

        // Priority 2: Programmatic KTraceConfigurer
        if (configurer != null && configurer.getTraceTopic() != null) {
            log.debug("ktrace trace-topic resolved from: KTraceConfigurer");
            return configurer.getTraceTopic();
        }

        // Priority 3: Default
        log.debug("ktrace trace-topic resolved from: default ({})", KTraceDefaults.TRACE_TOPIC);
        return KTraceDefaults.TRACE_TOPIC;
    }

    /**
     * Resolves async-queue-size with priority chain.
     */
    private int resolveAsyncQueueSize(
            KTraceProperties props,
            KTraceConfigurer configurer) {

        // Priority 1: ktrace.async-queue-size
        if (props.getAsyncQueueSize() != KTraceDefaults.ASYNC_QUEUE_SIZE) {
            log.debug("ktrace async-queue-size resolved from: ktrace.async-queue-size");
            return props.getAsyncQueueSize();
        }

        // Priority 2: Programmatic KTraceConfigurer
        if (configurer != null && configurer.getAsyncQueueSize() != null) {
            log.debug("ktrace async-queue-size resolved from: KTraceConfigurer");
            return configurer.getAsyncQueueSize();
        }

        // Priority 3: Default
        log.debug("ktrace async-queue-size resolved from: default ({})", KTraceDefaults.ASYNC_QUEUE_SIZE);
        return KTraceDefaults.ASYNC_QUEUE_SIZE;
    }

    /**
     * Resolves close-timeout-ms with priority chain.
     */
    private int resolveCloseTimeoutMs(
            KTraceProperties props,
            KTraceConfigurer configurer) {

        // Priority 1: ktrace.close-timeout-ms
        if (props.getCloseTimeoutMs() != KTraceDefaults.CLOSE_TIMEOUT_MS) {
            log.debug("ktrace close-timeout-ms resolved from: ktrace.close-timeout-ms");
            return props.getCloseTimeoutMs();
        }

        // Priority 2: Programmatic KTraceConfigurer
        if (configurer != null && configurer.getCloseTimeoutMs() != null) {
            log.debug("ktrace close-timeout-ms resolved from: KTraceConfigurer");
            return configurer.getCloseTimeoutMs();
        }

        // Priority 3: Default
        log.debug("ktrace close-timeout-ms resolved from: default ({})", KTraceDefaults.CLOSE_TIMEOUT_MS);
        return KTraceDefaults.CLOSE_TIMEOUT_MS;
    }

    /**
     * Resolves trace-topic-producer-retries with priority chain.
     */
    private int resolveTraceTopicProducerRetries(
            KTraceProperties props,
            KTraceConfigurer configurer) {

        // Priority 1: ktrace.trace-topic-producer-retries
        if (props.getTraceTopicProducerRetries() != KTraceDefaults.TRACE_TOPIC_PRODUCER_RETRIES) {
            log.debug("ktrace trace-topic-producer-retries resolved from: ktrace.trace-topic-producer-retries");
            return props.getTraceTopicProducerRetries();
        }

        // Priority 2: Programmatic KTraceConfigurer
        if (configurer != null && configurer.getTraceTopicProducerRetries() != null) {
            log.debug("ktrace trace-topic-producer-retries resolved from: KTraceConfigurer");
            return configurer.getTraceTopicProducerRetries();
        }

        // Priority 3: Default
        log.debug("ktrace trace-topic-producer-retries resolved from: default ({})",
                  KTraceDefaults.TRACE_TOPIC_PRODUCER_RETRIES);
        return KTraceDefaults.TRACE_TOPIC_PRODUCER_RETRIES;
    }
}
