package org.kpipe.config;

import static org.junit.jupiter.api.Assertions.*;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class AppConfigTest {

    @Test
    void shouldCreateConfigWithValidParameters() {
        // Arrange
        final var bootstrapServers = "localhost:9092";
        final var consumerGroup = "test-group";
        final var topic = "test-topic";
        final var appName = "test-app";
        final var pollTimeout = Duration.ofMillis(200);
        final var shutdownTimeout = Duration.ofSeconds(40);
        final var metricsInterval = Duration.ofMinutes(2);
        final var processors = List.of("processor1", "processor2");

        // Act
        final var config = new AppConfig(
                bootstrapServers,
                consumerGroup,
                topic,
                appName,
                pollTimeout,
                shutdownTimeout,
                metricsInterval,
                processors);

        // Assert
        assertEquals(bootstrapServers, config.bootstrapServers());
        assertEquals(consumerGroup, config.consumerGroup());
        assertEquals(topic, config.topic());
        assertEquals(appName, config.appName());
        assertEquals(pollTimeout, config.pollTimeout());
        assertEquals(shutdownTimeout, config.shutdownTimeout());
        assertEquals(metricsInterval, config.metricsInterval());
        assertEquals(processors, config.processors());
    }

    @Test
    void shouldUseDefaultValuesWhenParametersAreNull() {
        // Act
        final var config = new AppConfig(
                "localhost:9092",
                "test-group",
                "test-topic",
                "test-app",
                null,
                null,
                null,
                null);

        // Assert
        assertEquals(AppConfig.DEFAULT_POLL_TIMEOUT, config.pollTimeout());
        assertEquals(AppConfig.DEFAULT_SHUTDOWN_TIMEOUT, config.shutdownTimeout());
        assertEquals(AppConfig.DEFAULT_METRICS_INTERVAL, config.metricsInterval());
        assertTrue(config.processors().isEmpty());
    }

    @Test
    void shouldThrowExceptionForNullRequiredParameters() {
        // Assert
        assertThrows(NullPointerException.class, () -> new AppConfig(
                null, "group", "topic", "app", null, null, null, null));

        assertThrows(NullPointerException.class, () -> new AppConfig(
                "servers", null, "topic", "app", null, null, null, null));

        assertThrows(NullPointerException.class, () -> new AppConfig(
                "servers", "group", null, "app", null, null, null, null));

        assertThrows(NullPointerException.class, () -> new AppConfig(
                "servers", "group", "topic", null, null, null, null, null));
    }

    @Test
    void shouldThrowExceptionForNegativeDurations() {
        // Arrange
        final var negativeDuration = Duration.ofMillis(-1);

        // Assert
        assertThrows(IllegalArgumentException.class, () -> new AppConfig(
                "servers", "group", "topic", "app", negativeDuration, null, null, null));

        assertThrows(IllegalArgumentException.class, () -> new AppConfig(
                "servers", "group", "topic", "app", null, negativeDuration, null, null));

        assertThrows(IllegalArgumentException.class, () -> new AppConfig(
                "servers", "group", "topic", "app", null, null, negativeDuration, null));
    }

    @Test
    void shouldCreateConfigFromEnvironmentVariables() {
        // Arrange
        try (MockedStatic<AppConfig> mockedStatic = Mockito.mockStatic(AppConfig.class,
                Mockito.CALLS_REAL_METHODS)) {

            Map<String, String> envMap = Map.of(
                    "KAFKA_BOOTSTRAP_SERVERS", "test-server:9092",
                    "KAFKA_CONSUMER_GROUP", "env-group",
                    "KAFKA_TOPIC", "env-topic",
                    "APP_NAME", "env-app",
                    "KAFKA_POLL_TIMEOUT_MS", "250",
                    "SHUTDOWN_TIMEOUT_SEC", "45",
                    "METRICS_INTERVAL_SEC", "120",
                    "PROCESSOR_PIPELINE", "proc1,proc2,proc3"
            );

            mockedStatic.when(() -> AppConfig.getEnvOrDefault(Mockito.anyString(), Mockito.anyString()))
                    .thenAnswer(inv -> {
                        String key = inv.getArgument(0);
                        String defaultValue = inv.getArgument(1);
                        return envMap.getOrDefault(key, defaultValue);
                    });

            // Call the real fromEnv without actually using the mock to get code coverage
            // The assertions will verify our mocked values were used
            mockedStatic.when(AppConfig::fromEnv).thenCallRealMethod();

            // Act
            AppConfig config = AppConfig.fromEnv();

            // Assert
            assertEquals("test-server:9092", config.bootstrapServers());
            assertEquals("env-group", config.consumerGroup());
            assertEquals("env-topic", config.topic());
            assertEquals("env-app", config.appName());
            assertEquals(Duration.ofMillis(250), config.pollTimeout());
            assertEquals(Duration.ofSeconds(45), config.shutdownTimeout());
            assertEquals(Duration.ofSeconds(120), config.metricsInterval());
            assertEquals(List.of("proc1", "proc2", "proc3"), config.processors());
        }
    }

    @Test
    void shouldGetEnvironmentValueOrDefault() {
        // This test assumes environment variables might not be set in test environment

        // Act & Assert
        // For a variable likely not set in test environment
        final var randomKey = "RANDOM_TEST_KEY_" + System.currentTimeMillis();
        assertEquals("default-value", AppConfig.getEnvOrDefault(randomKey, "default-value"));

        // For a variable that may be set (like PATH)
        final var path = AppConfig.getEnvOrDefault("PATH", "no-path");
        assertNotNull(path);
    }
}