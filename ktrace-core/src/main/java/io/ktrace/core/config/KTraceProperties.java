package io.ktrace.core.config;

import java.io.InputStream;
import java.util.Properties;

/**
 * Centralized property loader for all ktrace configuration.
 * Loads once from all sources with priority:
 * <ol>
 *   <li>System properties (-Dktrace.enabled=true) - highest priority</li>
 *   <li>Environment variables (KTRACE_ENABLED=true)</li>
 *   <li>application.properties (classpath)</li>
 * </ol>
 * <p>
 * This class is loaded statically and shared across all ktrace components.
 */
final class KTraceProperties {

    private static final Properties PROPERTIES;

    static {
        PROPERTIES = loadFromAllSources();
    }

    private KTraceProperties() {
        // Utility class
    }

    /**
     * Gets a property value with fallback priority:
     * 1. System property (checked dynamically)
     * 2. Environment variable
     * 3. application.properties
     * 4. Default value
     *
     * @param key property key (e.g., "ktrace.enabled")
     * @param defaultValue default if not found
     * @return property value or default
     */
    public static String get(String key, String defaultValue) {
        // Always check system property first (dynamic, can be set at runtime)
        String sysProp = System.getProperty(key);
        if (sysProp != null) {
            return sysProp;
        }

        // Then check cached properties (env vars + file)
        return PROPERTIES.getProperty(key, defaultValue);
    }

    /**
     * Gets a boolean property.
     *
     * @param key property key
     * @param defaultValue default if not found
     * @return boolean value
     */
    public static boolean getBoolean(String key, boolean defaultValue) {
        String value = get(key, null);
        return (value != null) ? Boolean.parseBoolean(value) : defaultValue;
    }

    /**
     * Gets an int property.
     *
     * @param key property key
     * @param defaultValue default if not found
     * @return int value or default if parse fails
     */
    public static int getInt(String key, int defaultValue) {
        String value = get(key, null);
        if (value != null) {
            try {
                return Integer.parseInt(value);
            } catch (NumberFormatException e) {
                return defaultValue;
            }
        }
        return defaultValue;
    }

    /**
     * Loads properties from file and environment variables.
     * System properties are checked dynamically in get() method.
     */
    private static Properties loadFromAllSources() {
        Properties props = new Properties();

        // 1. Load from application.properties (lowest priority)
        loadFromClasspath(props);

        // 2. Override with environment variables
        loadFromEnvironment(props);

        // Note: System properties are NOT loaded here - they're checked dynamically
        // in get() to support runtime changes (important for tests)

        return props;
    }

    /**
     * Loads ktrace.* properties from application.properties on classpath.
     */
    private static void loadFromClasspath(Properties props) {
        try (InputStream is = KTraceProperties.class.getClassLoader()
                .getResourceAsStream("application.properties")) {
            if (is != null) {
                Properties fileProps = new Properties();
                fileProps.load(is);

                // Only copy ktrace.* properties
                fileProps.stringPropertyNames().stream()
                        .filter(key -> key.startsWith("ktrace."))
                        .forEach(key -> props.setProperty(key, fileProps.getProperty(key)));
            }
        } catch (Exception e) {
            // Ignore - file not available or not readable
        }
    }

    /**
     * Loads KTRACE_* environment variables.
     * Converts KTRACE_ENABLED -> ktrace.enabled
     */
    private static void loadFromEnvironment(Properties props) {
        System.getenv().forEach((envKey, envValue) -> {
            if (envKey.startsWith("KTRACE_")) {
                String propertyKey = envToPropertyKey(envKey);
                props.setProperty(propertyKey, envValue);
            }
        });
    }

    /**
     * Converts environment variable name to property key.
     * KTRACE_ENABLED -> ktrace.enabled
     * KTRACE_TRACE_TOPIC -> ktrace.trace-topic
     */
    private static String envToPropertyKey(String envKey) {
        return envKey.toLowerCase()
                .replace('_', '.')
                .replace("..", "-"); // TRACE_TOPIC -> trace.topic, then handle hyphens
    }
}
