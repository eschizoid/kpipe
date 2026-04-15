package org.kpipe.consumer;

import static org.junit.jupiter.api.Assertions.*;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.Test;
import org.kpipe.sink.KafkaMessageSink;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

@Testcontainers
class KPipeProducerIntegrationTest {

  private static final String KAFKA_VERSION = System.getProperty("kafkaVersion", "4.2.0");

  @Container
  static KafkaContainer kafka = new KafkaContainer(
    DockerImageName.parse("soldevelo/kafka:%s".formatted(KAFKA_VERSION))
                   .asCompatibleSubstituteFor("apache/kafka")
  );

  @Test
  void shouldSendToDlqOnFailure() throws Exception {
    final var topic = "input-" + System.nanoTime();
    final var dlqTopic = "dlq-" + System.nanoTime();
    final var props = getProperties();

    // 1. Produce message to input topic
    try (
      var producer = new KafkaProducer<byte[], byte[]>(props, new ByteArraySerializer(), new ByteArraySerializer())
    ) {
      producer.send(new ProducerRecord<>(topic, "key1".getBytes(), "bad-value".getBytes())).get();
    }

    // 2. Start consumer with DLQ
    final var consumer = KPipeConsumer.<byte[], byte[]>builder()
      .withProperties(props)
      .withTopic(topic)
      .withProcessor(v -> {
        if (new String(v).equals("bad-value")) throw new RuntimeException("Simulated failure");
        return v;
      })
      .withRetry(1, Duration.ofMillis(100))
      .withDeadLetterTopic(dlqTopic)
      .build();

    final var consumerThread = Thread.ofVirtual().start(consumer::start);

    try {
      // 3. Verify message reaches DLQ
      final var dlqMessage = consumeOne(props, dlqTopic, Duration.ofSeconds(15));
      assertNotNull(dlqMessage, "Message should be in DLQ");
      assertEquals("bad-value", new String(dlqMessage));
    } finally {
      consumer.close();
      consumerThread.join(5000);
    }
  }

  @Test
  void shouldSendToDlqWithExternalProducer() throws Exception {
    final var topic = "input-" + System.nanoTime();
    final var dlqTopic = "dlq-" + System.nanoTime();
    final var props = getProperties();
    final var externalProducer = new KafkaProducer<byte[], byte[]>(
      props,
      new ByteArraySerializer(),
      new ByteArraySerializer()
    );

    // 1. Produce message to input topic
    try (
      var producer = new KafkaProducer<byte[], byte[]>(props, new ByteArraySerializer(), new ByteArraySerializer())
    ) {
      producer.send(new ProducerRecord<>(topic, "key3".getBytes(), "bad-external".getBytes())).get();
    }

    // 2. Start consumer with DLQ and external producer
    final var consumer = KPipeConsumer.<byte[], byte[]>builder()
      .withProperties(props)
      .withTopic(topic)
      .withProcessor(v -> {
        if (new String(v).equals("bad-external")) throw new RuntimeException("Simulated failure");
        return v;
      })
      .withRetry(0, Duration.ZERO)
      .withDeadLetterTopic(dlqTopic)
      .withKafkaProducer(externalProducer)
      .build();

    final var consumerThread = Thread.ofVirtual().start(consumer::start);

    try {
      // 3. Verify message reaches DLQ
      final var dlqMessage = consumeOne(props, dlqTopic, Duration.ofSeconds(15));
      assertNotNull(dlqMessage, "Message should be in DLQ");
      assertEquals("bad-external", new String(dlqMessage));
    } finally {
      consumer.close();
      consumerThread.join(5000);
      externalProducer.close();
    }
  }

  @Test
  void shouldSendToOutputTopicViaSink() throws Exception {
    final var topic = "input-" + System.nanoTime();
    final var outputTopic = "output-" + System.nanoTime();
    final var props = getProperties();

    // 1. Produce message to input topic
    try (
      var producer = new KafkaProducer<byte[], byte[]>(props, new ByteArraySerializer(), new ByteArraySerializer())
    ) {
      producer.send(new ProducerRecord<>(topic, "key2".getBytes(), "good-value".getBytes())).get();
    }

    // 2. Start consumer with KafkaMessageSink
    final var consumer = KPipeConsumer.<byte[], byte[]>builder()
      .withProperties(props)
      .withTopic(topic)
      .withProcessor(v -> ("processed-" + new String(v)).getBytes())
      .withMessageSink(
        KafkaMessageSink.of(
          new KafkaProducer<>(props, new ByteArraySerializer(), new ByteArraySerializer()),
          outputTopic,
          v -> v
        )
      )
      .build();

    final var consumerThread = Thread.ofVirtual().start(consumer::start);

    try {
      // 3. Verify message reaches Output Topic
      final var outputMessage = consumeOne(props, outputTopic, Duration.ofSeconds(15));
      assertNotNull(outputMessage, "Message should be in Output Topic");
      assertEquals("processed-good-value", new String(outputMessage));
    } finally {
      consumer.close();
      consumerThread.join(5000);
    }
  }

  private Properties getProperties() {
    final var props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group-" + System.currentTimeMillis());
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
    return props;
  }

  private byte[] consumeOne(Properties props, String topic, Duration timeout) {
    try (
      var consumer = new KafkaConsumer<byte[], byte[]>(props, new ByteArrayDeserializer(), new ByteArrayDeserializer())
    ) {
      consumer.subscribe(Collections.singletonList(topic));
      final var start = System.currentTimeMillis();
      while (System.currentTimeMillis() - start < timeout.toMillis()) {
        final var records = consumer.poll(Duration.ofMillis(100));
        if (!records.isEmpty()) {
          return records.iterator().next().value();
        }
      }
    }
    return null;
  }
}
