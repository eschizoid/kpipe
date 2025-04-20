package org.kpipe.config;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Properties;
import org.junit.jupiter.api.Test;

class KafkaConsumerConfigTest {

    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String GROUP_ID = "test-group";

    @Test
    void shouldCreateDefaultConsumerConfig() {
        // Act
        final var props = KafkaConsumerConfig.createConsumerConfig(BOOTSTRAP_SERVERS, GROUP_ID);

        // Assert
        assertEquals(BOOTSTRAP_SERVERS, props.get("bootstrap.servers"));
        assertEquals(GROUP_ID, props.get("group.id"));
        assertEquals("org.apache.kafka.common.serialization.ByteArrayDeserializer", props.get("key.deserializer"));
        assertEquals("org.apache.kafka.common.serialization.ByteArrayDeserializer", props.get("value.deserializer"));
        assertEquals("true", props.get("enable.auto.commit"));
    }

    @Test
    void shouldApplyCustomizer() {
        // Act
        final var props = KafkaConsumerConfig.createConsumerConfig(
                BOOTSTRAP_SERVERS,
                GROUP_ID,
                p -> {
                    p.put("max.poll.records", "100");
                    return p;
                });

        // Assert
        assertEquals(BOOTSTRAP_SERVERS, props.get("bootstrap.servers"));
        assertEquals(GROUP_ID, props.get("group.id"));
        assertEquals("100", props.get("max.poll.records"));
    }

    @Test
    void shouldApplyCustomDeserializers() {
        // Arrange
        final var keyDeserializer = "org.apache.kafka.common.serialization.StringDeserializer";
        final var valueDeserializer = "org.apache.kafka.common.serialization.StringDeserializer";

        // Act
        final var props = KafkaConsumerConfig.createConsumerConfig(
                BOOTSTRAP_SERVERS,
                GROUP_ID,
                KafkaConsumerConfig.withCustomDeserializers(keyDeserializer, valueDeserializer));

        // Assert
        assertEquals(keyDeserializer, props.get("key.deserializer"));
        assertEquals(valueDeserializer, props.get("value.deserializer"));
    }

    @Test
    void shouldDisableAutoCommit() {
        // Act
        final var props = KafkaConsumerConfig.createConsumerConfig(
                BOOTSTRAP_SERVERS,
                GROUP_ID,
                KafkaConsumerConfig.withAutoCommitDisabled());

        // Assert
        assertEquals("false", props.get("enable.auto.commit"));
    }

    @Test
    void shouldSetCustomProperty() {
        // Act
        final var props = KafkaConsumerConfig.createConsumerConfig(
                BOOTSTRAP_SERVERS,
                GROUP_ID,
                KafkaConsumerConfig.withProperty("fetch.min.bytes", "1000"));

        // Assert
        assertEquals("1000", props.get("fetch.min.bytes"));
    }

    @Test
    void shouldBuildConfigurationWithBuilder() {
        // Act
        final var props = KafkaConsumerConfig.consumerBuilder()
                .withBootstrapServers(BOOTSTRAP_SERVERS)
                .withGroupId(GROUP_ID)
                .withByteArrayDeserializers()
                .withAutoCommit(false)
                .withProperty("max.poll.records", "100")
                .build();

        // Assert
        assertEquals(BOOTSTRAP_SERVERS, props.get("bootstrap.servers"));
        assertEquals(GROUP_ID, props.get("group.id"));
        assertEquals("org.apache.kafka.common.serialization.ByteArrayDeserializer", props.get("key.deserializer"));
        assertEquals("org.apache.kafka.common.serialization.ByteArrayDeserializer", props.get("value.deserializer"));
        assertEquals("false", props.get("enable.auto.commit"));
        assertEquals("100", props.get("max.poll.records"));
    }
}