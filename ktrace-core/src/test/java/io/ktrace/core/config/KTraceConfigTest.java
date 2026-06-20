package io.ktrace.core.config;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.*;

class KTraceConfigTest {

    private final KTraceConfig originalConfig = KTraceConfig.getInstance();

    @AfterEach
    void restoreConfig() {
        KTraceConfig.setInstance(originalConfig);
    }

    @Test
    void constructor_shouldSetLoggingFlag() {
        KTraceConfig config = new KTraceConfig(true);

        assertThat(config.isLoggingEnabled()).isTrue();
    }

    @Test
    void constructor_withFalse_shouldDisableLogging() {
        KTraceConfig config = new KTraceConfig(false);

        assertThat(config.isLoggingEnabled()).isFalse();
    }

    @Test
    void getInstance_shouldReturnGlobalInstance() {
        KTraceConfig instance = KTraceConfig.getInstance();

        assertThat(instance).isNotNull();
        assertThat(instance.isLoggingEnabled()).isFalse();  // Default disabled
    }

    @Test
    void setInstance_shouldUpdateGlobalInstance() {
        KTraceConfig newConfig = new KTraceConfig(false);

        KTraceConfig.setInstance(newConfig);

        assertThat(KTraceConfig.getInstance()).isSameAs(newConfig);
        assertThat(KTraceConfig.getInstance().isLoggingEnabled()).isFalse();
    }

    @Test
    void setInstance_withNull_shouldThrowNullPointerException() {
        assertThatThrownBy(() -> KTraceConfig.setInstance(null))
                .isInstanceOf(NullPointerException.class)
                .hasMessageContaining("config must not be null");
    }

    @Test
    void defaultConfig_shouldHaveLoggingDisabled() {
        // Default instance should have logging disabled
        assertThat(KTraceConfig.getInstance().isLoggingEnabled()).isFalse();
    }

    @Test
    void setInstance_shouldOverrideLoadedConfig() {
        // Even if config was loaded from properties, setInstance should override
        KTraceConfig enabled = new KTraceConfig(true);
        KTraceConfig.setInstance(enabled);

        assertThat(KTraceConfig.getInstance().isLoggingEnabled()).isTrue();

        KTraceConfig disabled = new KTraceConfig(false);
        KTraceConfig.setInstance(disabled);

        assertThat(KTraceConfig.getInstance().isLoggingEnabled()).isFalse();
    }
}
