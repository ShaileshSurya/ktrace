package io.ktrace.core.config;

import java.io.InputStream;
import java.util.Properties;

/**
 * Configuration for ktrace behavior.
 * <p>
 * Controls whether trace context is logged to application logs (MDC).
 * Trace headers are ALWAYS injected regardless of this setting.
 * <p>
 * Configuration is loaded in the following priority order:
 * <ol>
 *   <li>Programmatic: {@code KTraceConfig.setInstance(new KTraceConfig(true))}</li>
 *   <li>System property: {@code -Dktrace.mdc.enabled=true}</li>
 *   <li>Environment variable: {@code KTRACE_MDC_ENABLED=true}</li>
 *   <li>Properties file: {@code application.properties} on classpath</li>
 *   <li>Default: {@code false} (disabled)</li>
 * </ol>
 */
public final class KTraceConfig {

    private static volatile KTraceConfig instance;

    static {
        instance = loadFromProperties();
    }

    private final boolean loggingEnabled;

    /**
     * Creates a new KTraceConfig.
     *
     * @param loggingEnabled whether to include trace context in application logs
     */
    public KTraceConfig(boolean loggingEnabled) {
        this.loggingEnabled = loggingEnabled;
    }

    /**
     * Gets the global KTraceConfig instance.
     *
     * @return the current config
     */
    public static KTraceConfig getInstance() {
        return instance;
    }

    /**
     * Sets the global KTraceConfig instance.
     * <p>
     * This overrides any configuration loaded from properties, system properties,
     * or environment variables.
     *
     * @param config the config to use
     */
    public static void setInstance(KTraceConfig config) {
        if (config == null) {
            throw new NullPointerException("config must not be null");
        }
        instance = config;
    }

    /**
     * Checks if trace context should be logged to MDC.
     *
     * @return true if logging is enabled, false otherwise
     */
    public boolean isLoggingEnabled() {
        return loggingEnabled;
    }

    /**
     * Loads configuration from properties in priority order:
     * <ol>
     *   <li>System property: {@code -Dktrace.mdc.enabled=true}</li>
     *   <li>Environment variable: {@code KTRACE_MDC_ENABLED=true}</li>
     *   <li>Properties file: {@code application.properties} on classpath</li>
     *   <li>Default: {@code false} (disabled)</li>
     * </ol>
     *
     * @return the loaded configuration
     */
    private static KTraceConfig loadFromProperties() {
        // 1. Check system property first (highest priority)
        String sysProp = System.getProperty("ktrace.mdc.enabled");
        if (sysProp != null) {
            return new KTraceConfig(Boolean.parseBoolean(sysProp));
        }

        // 2. Check environment variable
        String envVar = System.getenv("KTRACE_MDC_ENABLED");
        if (envVar != null) {
            return new KTraceConfig(Boolean.parseBoolean(envVar));
        }

        // 3. Check application.properties file on classpath
        try (InputStream is = KTraceConfig.class.getClassLoader()
                .getResourceAsStream("application.properties")) {
            if (is != null) {
                Properties props = new Properties();
                props.load(is);
                String fileProp = props.getProperty("ktrace.mdc.enabled");
                if (fileProp != null) {
                    return new KTraceConfig(Boolean.parseBoolean(fileProp));
                }
            }
        } catch (Exception e) {
            // Ignore - use default
        }

        // 4. Default: disabled
        return new KTraceConfig(false);
    }
}
