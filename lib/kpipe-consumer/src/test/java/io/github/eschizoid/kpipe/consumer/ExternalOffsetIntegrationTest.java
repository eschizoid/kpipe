package io.github.eschizoid.kpipe.consumer;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.*;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.postgresql.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

/// Offsets kept in Postgres through an [OffsetStore] instead of Kafka.
@Testcontainers
class ExternalOffsetIntegrationTest {

  private static final String KAFKA_VERSION = System.getProperty("kafkaVersion", "4.3.0");
  private static final String TOPIC = "payments-topic";
  private static final String GROUP = "payment-service";
  private static final TopicPartition PARTITION = new TopicPartition(TOPIC, 0);
  private static final Duration COMMIT_INTERVAL = Duration.ofMillis(100);
  private static final int STALL_FROM = 150;

  @Container
  private static final PostgreSQLContainer postgres = new PostgreSQLContainer("postgres:16-alpine")
    .withDatabaseName("kpipe_test")
    .withUsername("testuser")
    .withPassword("testpass");

  @Container
  private static final KafkaContainer kafka = new KafkaContainer(
    DockerImageName.parse("apache/kafka:%s".formatted(KAFKA_VERSION))
  );

  private Properties properties;

  @BeforeEach
  void setUp() throws SQLException {
    properties = new Properties();
    properties.put("bootstrap.servers", "localhost:9092");
    properties.put("group.id", GROUP);

    try (final var conn = connect(); final var stmt = conn.createStatement()) {
      stmt.execute(
        "CREATE TABLE IF NOT EXISTS consumer_offsets (group_id TEXT, topic TEXT, partition INT, next_offset BIGINT, " +
          "PRIMARY KEY (group_id, topic, partition))"
      );
      stmt.execute("DELETE FROM consumer_offsets");
    }
  }

  /// An [OffsetStore] backed by a Postgres table keyed by group, topic and partition. Each
  /// row holds the next offset to read.
  static final class PostgresOffsetStore implements OffsetStore {

    private final String groupId;

    PostgresOffsetStore(final String groupId) {
      this.groupId = groupId;
    }

    @Override
    public Map<TopicPartition, Long> load(final Collection<TopicPartition> partitions) {
      final var offsets = new HashMap<TopicPartition, Long>();
      try (
        final var conn = connect();
        final var stmt = conn.prepareStatement(
          "SELECT next_offset FROM consumer_offsets WHERE group_id = ? AND topic = ? AND partition = ?"
        )
      ) {
        for (final var partition : partitions) {
          stmt.setString(1, groupId);
          stmt.setString(2, partition.topic());
          stmt.setInt(3, partition.partition());
          try (final var rs = stmt.executeQuery()) {
            if (rs.next()) offsets.put(partition, rs.getLong(1));
          }
        }
        return offsets;
      } catch (final SQLException e) {
        throw new IllegalStateException("Failed to load offsets", e);
      }
    }

    @Override
    public void commit(final Map<TopicPartition, Long> offsets) {
      try (
        final var conn = connect();
        final var stmt = conn.prepareStatement(
          "INSERT INTO consumer_offsets (group_id, topic, partition, next_offset) VALUES (?, ?, ?, ?) " +
            "ON CONFLICT (group_id, topic, partition) DO UPDATE SET next_offset = EXCLUDED.next_offset"
        )
      ) {
        conn.setAutoCommit(false);
        for (final var entry : offsets.entrySet()) {
          stmt.setString(1, groupId);
          stmt.setString(2, entry.getKey().topic());
          stmt.setInt(3, entry.getKey().partition());
          stmt.setLong(4, entry.getValue());
          stmt.addBatch();
        }
        stmt.executeBatch();
        conn.commit();
      } catch (final SQLException e) {
        throw new IllegalStateException("Failed to store offsets", e);
      }
    }
  }

  /// Wraps a store and records the highest offset it was asked to commit for each partition.
  static final class RecordingStore implements OffsetStore {

    private final OffsetStore delegate;
    final Map<TopicPartition, Long> highestCommitted = new ConcurrentHashMap<>();
    final AtomicInteger commits = new AtomicInteger();

    RecordingStore(final OffsetStore delegate) {
      this.delegate = delegate;
    }

    @Override
    public Map<TopicPartition, Long> load(final Collection<TopicPartition> partitions) {
      return delegate.load(partitions);
    }

    @Override
    public void commit(final Map<TopicPartition, Long> offsets) {
      delegate.commit(offsets);
      offsets.forEach((partition, offset) -> highestCommitted.merge(partition, offset, Math::max));
      commits.incrementAndGet();
    }
  }

  @Test
  void resumesFromTheStoredOffset() throws Exception {
    final var store = new PostgresOffsetStore(GROUP);
    store.commit(Map.of(PARTITION, 5L));
    final var mc = new MockConsumer<byte[], byte[]>("earliest");
    mc.updateBeginningOffsets(Map.of(PARTITION, 0L));
    final var processed = ConcurrentHashMap.<String>newKeySet();
    final var lastRecordDone = new CountDownLatch(1);

    final var consumer = mockConsumer(mc, store, ProcessingMode.SEQUENTIAL, text -> {
      processed.add(text);
      if ("v9".equals(text)) lastRecordDone.countDown();
    });
    consumer.start();
    mc.rebalance(List.of(PARTITION));
    addRecords(mc, 10);

    assertTrue(lastRecordDone.await(20, TimeUnit.SECONDS), "v9 should be processed");
    consumer.close();
    assertEquals(values(5, 10), processed, "only the records from the stored offset on are processed");
  }

  /// PARALLEL finishes records out of order. While v5 is still running and every other record has
  /// finished, the store must hold 5, not 10; after a restart, v5 is processed again and the
  /// records before it are not.
  @Test
  void restartReprocessesARecordThatWasStillRunningWhenALaterOneFinished() throws Exception {
    final var firstRun = new MockConsumer<byte[], byte[]>("earliest");
    firstRun.updateBeginningOffsets(Map.of(PARTITION, 0L));
    final var firstStore = new RecordingStore(new PostgresOffsetStore(GROUP));
    final var neverReleased = new CountDownLatch(1);
    final var othersDone = new CountDownLatch(9);
    final var first = mockConsumer(firstRun, firstStore, ProcessingMode.PARALLEL, text -> {
      if (!"v5".equals(text)) {
        othersDone.countDown();
        return;
      }
      try {
        neverReleased.await();
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IllegalStateException(e);
      }
    });
    first.start();
    firstRun.rebalance(List.of(PARTITION));
    addRecords(firstRun, 10);
    assertTrue(othersDone.await(20, TimeUnit.SECONDS), "every record except v5 should finish");
    final var commitsAfterOthers = firstStore.commits.get();
    awaitTrue(() -> firstStore.commits.get() >= commitsAfterOthers + 2, "two commits after the others finished");
    first.close();

    assertEquals(5L, firstStore.highestCommitted.get(PARTITION), "the store never moves past the running v5");

    final var secondRun = new MockConsumer<byte[], byte[]>("earliest");
    secondRun.updateBeginningOffsets(Map.of(PARTITION, 0L));
    final var reprocessed = ConcurrentHashMap.<String>newKeySet();
    final var lastRecordDone = new CountDownLatch(1);
    final var second = mockConsumer(secondRun, new PostgresOffsetStore(GROUP), ProcessingMode.SEQUENTIAL, text -> {
      reprocessed.add(text);
      if ("v9".equals(text)) lastRecordDone.countDown();
    });
    second.start();
    secondRun.rebalance(List.of(PARTITION));
    addRecords(secondRun, 10);
    assertTrue(lastRecordDone.await(20, TimeUnit.SECONDS), "v9 should be processed after the restart");
    second.close();

    assertEquals(values(5, 10), reprocessed, "the restart resumes at v5, the record that never finished");
  }

  /// Against a real broker, a restart resumes from the offset in Postgres and never from Kafka: the
  /// group commits nothing to Kafka, so without the seek the restart would start from the
  /// beginning under `auto.offset.reset=earliest`.
  @Test
  void restartAgainstARealBrokerResumesFromTheStore() throws Exception {
    final var topic = "store-restart-" + System.nanoTime();
    final var groupId = "store-group-" + System.nanoTime();
    final var recordCount = 300;
    final var partition = new TopicPartition(topic, 0);
    try (final var admin = Admin.create(Map.of("bootstrap.servers", kafka.getBootstrapServers()))) {
      admin.createTopics(List.of(new NewTopic(topic, 1, (short) 1))).all().get(30, TimeUnit.SECONDS);
    }
    try (
      final var producer = new KafkaProducer<>(
        Map.<String, Object>of("bootstrap.servers", kafka.getBootstrapServers()),
        new ByteArraySerializer(),
        new ByteArraySerializer()
      )
    ) {
      for (int i = 0; i < recordCount; i++) producer.send(new ProducerRecord<>(topic, ("v" + i).getBytes(UTF_8)));
      producer.flush();
    }

    final var firstSeen = ConcurrentHashMap.<String>newKeySet();
    final var first = realConsumer(topic, groupId, firstSeen, STALL_FROM);
    first.start();
    awaitTrue(
      () -> firstSeen.size() == STALL_FROM,
      "the first run finishes every record before v%d".formatted(STALL_FROM)
    );
    first.close();

    final var stored = new PostgresOffsetStore(groupId).load(List.of(partition)).get(partition);
    assertNotNull(stored, "the first run stores an offset");
    assertTrue(
      stored <= STALL_FROM,
      "the store never moves past the stalled v%d; stored %d".formatted(STALL_FROM, stored)
    );
    for (int i = 0; i < stored; i++) {
      assertTrue(firstSeen.contains("v" + i), "v%d is below the stored offset, so it finished".formatted(i));
    }

    final var secondSeen = ConcurrentHashMap.<String>newKeySet();
    final var second = realConsumer(topic, groupId, secondSeen, Integer.MAX_VALUE);
    second.start();
    awaitTrue(() -> secondSeen.contains("v" + (recordCount - 1)), "the restart reaches the last record");
    second.close();

    assertEquals(
      values(Math.toIntExact(stored), recordCount),
      secondSeen,
      "the restart processes exactly the records from the stored offset on"
    );
    try (final var admin = Admin.create(Map.of("bootstrap.servers", kafka.getBootstrapServers()))) {
      final var kafkaCommitted = admin
        .listConsumerGroupOffsets(groupId)
        .partitionsToOffsetAndMetadata()
        .get(15, TimeUnit.SECONDS);
      assertTrue(kafkaCommitted.isEmpty(), "offsets live in Postgres only; Kafka has %s".formatted(kafkaCommitted));
    }
  }

  private KPipeConsumer mockConsumer(
    final MockConsumer<byte[], byte[]> mc,
    final OffsetStore store,
    final ProcessingMode mode,
    final java.util.function.Consumer<String> onRecord
  ) {
    return KPipeConsumer.builder()
      .withProperties(properties)
      .withTopic(TOPIC)
      .withConsumer(() -> mc)
      .withOffsetStore(store)
      .withCommitInterval(COMMIT_INTERVAL)
      .withPipeline(
        TestPipelines.sideEffect(value -> {
          onRecord.accept(new String(value, UTF_8));
          return value;
        })
      )
      .withProcessingMode(mode)
      .withWaitForMessagesTimeout(Duration.ofMillis(500))
      .build();
  }

  /// Builds a PARALLEL consumer whose records finish in random order. Records from `stallFrom` on
  /// block until the consumer closes, so a run stops mid-stream with records still running.
  private static KPipeConsumer realConsumer(
    final String topic,
    final String groupId,
    final Set<String> seen,
    final int stallFrom
  ) {
    final var props = new Properties();
    props.put("bootstrap.servers", kafka.getBootstrapServers());
    props.put("group.id", groupId);
    props.put("auto.offset.reset", "earliest");
    props.put("max.poll.records", "20");
    props.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
    return KPipeConsumer.builder()
      .withProperties(props)
      .withTopic(topic)
      .withOffsetStore(new PostgresOffsetStore(groupId))
      .withCommitInterval(COMMIT_INTERVAL)
      .withProcessingMode(ProcessingMode.PARALLEL)
      .withPipeline(
        TestPipelines.sideEffect(value -> {
          final var text = new String(value, UTF_8);
          try {
            if (Integer.parseInt(text.substring(1)) >= stallFrom) new CountDownLatch(1).await();
            TimeUnit.MILLISECONDS.sleep(ThreadLocalRandom.current().nextInt(1, 10));
          } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
          }
          seen.add(text);
          return value;
        })
      )
      .build();
  }

  private static Connection connect() throws SQLException {
    return DriverManager.getConnection(postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
  }

  private static void addRecords(final MockConsumer<byte[], byte[]> mc, final int count) {
    for (int i = 0; i < count; i++) {
      mc.addRecord(new ConsumerRecord<>(TOPIC, 0, i, ("k" + i).getBytes(UTF_8), ("v" + i).getBytes(UTF_8)));
    }
  }

  private static Set<String> values(final int fromInclusive, final int toExclusive) {
    return IntStream.range(fromInclusive, toExclusive)
      .mapToObj(i -> "v" + i)
      .collect(Collectors.toSet());
  }

  private static void awaitTrue(final BooleanSupplier condition, final String description) throws InterruptedException {
    final var deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
    while (!condition.getAsBoolean()) {
      if (System.nanoTime() > deadline) fail("timed out waiting until " + description);
      Thread.sleep(50);
    }
  }
}
