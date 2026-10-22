package io.github.eschizoid.kpipe.consumer;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

/// A consumer built without an explicit [OffsetManager] still commits the offsets of the records
/// it finished, and never leaves Kafka's auto-commit on, whatever the supplied properties say.
class DefaultOffsetManagerTest {

  private static final String TOPIC = "default-offsets";
  private static final TopicPartition PARTITION = new TopicPartition(TOPIC, 0);
  private static final int RECORDS = 10;

  @ParameterizedTest
  @EnumSource(ProcessingMode.class)
  void defaultConsumerCommitsEveryFinishedRecordOnClose(final ProcessingMode mode) throws InterruptedException {
    final var commits = new ConcurrentHashMap<TopicPartition, OffsetAndMetadata>();
    final var mockConsumer = new MockConsumer<byte[], byte[]>("earliest") {
      @Override
      public synchronized void commitSync(final Map<TopicPartition, OffsetAndMetadata> offsets) {
        super.commitSync(offsets);
        commits.putAll(offsets);
      }

      @Override
      public synchronized void commitSync(
        final Map<TopicPartition, OffsetAndMetadata> offsets,
        final Duration timeout
      ) {
        super.commitSync(offsets, timeout);
        commits.putAll(offsets);
      }

      @Override
      public synchronized void subscribe(final Collection<String> topics) {}

      @Override
      public synchronized void subscribe(final Collection<String> topics, final ConsumerRebalanceListener callback) {}
    };
    mockConsumer.assign(List.of(PARTITION));
    mockConsumer.updateBeginningOffsets(Map.of(PARTITION, 0L));
    for (int i = 0; i < RECORDS; i++) {
      mockConsumer.addRecord(new ConsumerRecord<>(TOPIC, 0, i, ("k" + i).getBytes(UTF_8), "v".getBytes(UTF_8)));
    }

    final var processed = new AtomicInteger();
    final var consumer = KPipeConsumer.builder()
      .withProperties(properties("false"))
      .withTopic(TOPIC)
      .withPipeline(
        TestPipelines.sideEffect(b -> {
          processed.incrementAndGet();
          return b;
        })
      )
      .withProcessingMode(mode)
      .withConsumer(() -> mockConsumer)
      .build();

    try {
      consumer.start();
      TestAwaits.pollUntil(() -> processed.get() == RECORDS, Duration.ofSeconds(10), "every record is processed");
    } finally {
      consumer.close();
    }

    final var committed = commits.get(PARTITION);
    assertNotNull(committed, "no offset committed for " + PARTITION + " in " + mode);
    assertEquals(RECORDS, committed.offset(), "committed offset in " + mode);
  }

  @ParameterizedTest
  @ValueSource(strings = { "true", "TRUE", " true " })
  void buildDisablesAutoCommitWhateverThePropertiesSay(final String autoCommit) {
    final var builder = KPipeConsumer.builder()
      .withProperties(properties(autoCommit))
      .withTopic(TOPIC)
      .withPipeline(TestPipelines.identity())
      .withConsumer(() -> new MockConsumer<byte[], byte[]>("earliest"));

    try (final var _ = builder.build()) {
      assertEquals("false", builder.kafkaProps.get("enable.auto.commit"));
    }
  }

  private static Properties properties(final String autoCommit) {
    final var properties = new Properties();
    properties.put("bootstrap.servers", "localhost:9092");
    properties.put("group.id", "default-offsets-group");
    properties.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
    properties.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
    properties.put("enable.auto.commit", autoCommit);
    return properties;
  }
}
