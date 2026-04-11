package org.kpipe.producer.config;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

class KafkaProducerConfigTest {

  private static final String BOOTSTRAP_SERVERS = "localhost:9092";

  @Test
  void shouldCreateDefaultProducerConfig() {
    // Act
    final var props = KafkaProducerConfig.createProducerConfig(BOOTSTRAP_SERVERS);

    // Assert
    assertEquals(BOOTSTRAP_SERVERS, props.get("bootstrap.servers"));
    assertEquals("org.apache.kafka.common.serialization.ByteArraySerializer", props.get("key.serializer"));
    assertEquals("org.apache.kafka.common.serialization.ByteArraySerializer", props.get("value.serializer"));
    assertEquals("1", props.get("acks"));
  }

  @Test
  void shouldApplyCustomizer() {
    // Act
    final var props = KafkaProducerConfig.createProducerConfig(BOOTSTRAP_SERVERS, p -> {
      p.put("retries", "10");
      return p;
    });

    // Assert
    assertEquals(BOOTSTRAP_SERVERS, props.get("bootstrap.servers"));
    assertEquals("10", props.get("retries"));
  }

  @Test
  void shouldApplyCustomSerializers() {
    // Arrange
    final var keySerializer = "org.apache.kafka.common.serialization.StringSerializer";
    final var valueSerializer = "org.apache.kafka.common.serialization.StringSerializer";

    // Act
    final var props = KafkaProducerConfig.createProducerConfig(
      BOOTSTRAP_SERVERS,
      KafkaProducerConfig.withCustomSerializers(keySerializer, valueSerializer)
    );

    // Assert
    assertEquals(keySerializer, props.get("key.serializer"));
    assertEquals(valueSerializer, props.get("value.serializer"));
  }

  @Test
  void shouldSetAcks() {
    // Act
    final var props = KafkaProducerConfig.createProducerConfig(BOOTSTRAP_SERVERS, KafkaProducerConfig.withAcks("all"));

    // Assert
    assertEquals("all", props.get("acks"));
  }

  @Test
  void shouldSetCustomProperty() {
    // Act
    final var props = KafkaProducerConfig.createProducerConfig(
      BOOTSTRAP_SERVERS,
      KafkaProducerConfig.withProperty("compression.type", "snappy")
    );

    // Assert
    assertEquals("snappy", props.get("compression.type"));
  }

  @Test
  void shouldApplyWithCustomizerInBuilder() {
    // Act
    final var props = KafkaProducerConfig.producerBuilder()
      .withBootstrapServers(BOOTSTRAP_SERVERS)
      .with(p -> {
        p.put("custom.prop", "val");
        return p;
      })
      .build();

    // Assert
    assertEquals(BOOTSTRAP_SERVERS, props.get("bootstrap.servers"));
    assertEquals("val", props.get("custom.prop"));
  }

  @Test
  void shouldBuildConfigurationWithBuilder() {
    // Act
    final var props = KafkaProducerConfig.producerBuilder()
      .withBootstrapServers(BOOTSTRAP_SERVERS)
      .withByteArraySerializers()
      .withAcks("all")
      .withProperty("retries", "10")
      .build();

    // Assert
    assertEquals(BOOTSTRAP_SERVERS, props.get("bootstrap.servers"));
    assertEquals("org.apache.kafka.common.serialization.ByteArraySerializer", props.get("key.serializer"));
    assertEquals("org.apache.kafka.common.serialization.ByteArraySerializer", props.get("value.serializer"));
    assertEquals("all", props.get("acks"));
    assertEquals("10", props.get("retries"));
  }
}
