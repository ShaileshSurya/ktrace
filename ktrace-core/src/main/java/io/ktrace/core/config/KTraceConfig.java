package io.ktrace.core.config;

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
        // Delegate to centralized KTraceProperties loader
        boolean mdcEnabled = KTraceProperties.getBoolean("ktrace.mdc.enabled", false);
        return new KTraceConfig(mdcEnabled);
    }
}
