package org.kpipe.consumer;

import static org.junit.jupiter.api.Assertions.*;

import java.time.Duration;
import java.util.Properties;
import java.util.function.Function;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class FunctionalKafkaConsumerTest {

  private static final String TOPIC = "test-topic";
  private static final Duration POLL_TIMEOUT = Duration.ofMillis(100);

  @Mock
  private Function<String, String> mockProcessor;

  private Properties properties;

  @BeforeEach
  void setUp() {
    properties = new Properties();
    properties.put("bootstrap.servers", "localhost:9092");
    properties.put("group.id", "test-group");
    properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    properties.put("enable.auto.commit", "true");
  }

  @Test
  void constructor_WithValidParameters_ShouldNotThrowException() {
    assertDoesNotThrow(() -> new FunctionalConsumer<>(properties, TOPIC, mockProcessor));
    assertDoesNotThrow(() -> new FunctionalConsumer<>(properties, TOPIC, mockProcessor, POLL_TIMEOUT));
  }

  @Test
  void constructor_WithNullParameters_ShouldThrowNullPointerException() {
    assertThrows(NullPointerException.class, () -> new FunctionalConsumer<>(null, TOPIC, mockProcessor));
    assertThrows(NullPointerException.class, () -> new FunctionalConsumer<>(properties, null, mockProcessor));
    assertThrows(NullPointerException.class, () -> new FunctionalConsumer<>(properties, TOPIC, null));
    assertThrows(NullPointerException.class, () -> new FunctionalConsumer<>(properties, TOPIC, mockProcessor, null));
  }

  @Test
  void isRunningShouldReturnTrueAfterConstruction() {
    try (final var consumer = new FunctionalConsumer<>(properties, TOPIC, mockProcessor)) {
      assertTrue(consumer.isRunning());
    }
  }

  @Test
  void isRunningShouldReturnFalseAfterClose() {
    final var consumer = new FunctionalConsumer<>(properties, TOPIC, mockProcessor);
    consumer.close();
    assertFalse(consumer.isRunning());
  }

  @Test
  void closeCalledMultipleTimesShouldBeIdempotent() {
    final var consumer = new FunctionalConsumer<>(properties, TOPIC, mockProcessor);
    assertTrue(consumer.isRunning());
    consumer.close();
    assertFalse(consumer.isRunning());
    // Second close should not change state
    consumer.close();
    assertFalse(consumer.isRunning());
  }

  @Test
  void autoCloseableShouldCloseConsumerWhenExitingTryWithResources() {
    final FunctionalConsumer<String, String> consumer;
    try (FunctionalConsumer<String, String> c = new FunctionalConsumer<>(properties, TOPIC, mockProcessor)) {
      consumer = c;
      assertTrue(consumer.isRunning());
    }
    assertFalse(consumer.isRunning());
  }
}
