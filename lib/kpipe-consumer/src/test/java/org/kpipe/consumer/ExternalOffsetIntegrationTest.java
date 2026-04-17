package org.kpipe.consumer;

import static org.junit.jupiter.api.Assertions.*;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.*;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.kpipe.consumer.enums.OffsetState;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;

@Testcontainers
class ExternalOffsetIntegrationTest {

  private static final String TOPIC = "payments-topic";
  private static final TopicPartition PARTITION = new TopicPartition(TOPIC, 0);
  private Properties properties;

  @Container
  private static final PostgreSQLContainer postgres = new PostgreSQLContainer("postgres:16-alpine")
    .withDatabaseName("kpipe_test")
    .withUsername("testuser")
    .withPassword("testpass");

  @BeforeEach
  void setUp() throws SQLException {
    properties = new Properties();
    properties.put("bootstrap.servers", "localhost:9092");
    properties.put("group.id", "payment-service");
    properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

    try (
      final var conn = DriverManager.getConnection(
        postgres.getJdbcUrl(),
        postgres.getUsername(),
        postgres.getPassword()
      );
      final var stmt = conn.createStatement()
    ) {
      stmt.execute(
        "CREATE TABLE IF NOT EXISTS consumer_offsets (topic TEXT, partition INT, last_offset BIGINT, PRIMARY KEY (topic, partition))"
      );
      stmt.execute("DELETE FROM consumer_offsets");
    }
  }

  /// A Database-backed Offset Manager using PostgreSQL.
  static class PostgresOffsetManager<K, V> implements OffsetManager<K, V> {

    private final String jdbcUrl;
    private final String username;
    private final String password;
    private OffsetState state = OffsetState.CREATED;
    private final MockConsumer<K, V> consumer;

    public PostgresOffsetManager(
      final MockConsumer<K, V> consumer,
      final String jdbcUrl,
      final String username,
      final String password
    ) {
      this.consumer = consumer;
      this.jdbcUrl = jdbcUrl;
      this.username = username;
      this.password = password;
    }

    @Override
    public OffsetManager<K, V> start() {
      this.state = OffsetState.RUNNING;
      return this;
    }

    @Override
    public OffsetManager<K, V> stop() {
      this.state = OffsetState.STOPPED;
      return this;
    }

    @Override
    public void trackOffset(ConsumerRecord<K, V> record) {}

    @Override
    public void markOffsetProcessed(ConsumerRecord<K, V> record) {
      try (
        final var conn = DriverManager.getConnection(jdbcUrl, username, password);
        final var stmt = conn.prepareStatement(
          "INSERT INTO consumer_offsets (topic, partition, last_offset) VALUES (?, ?, ?) " +
            "ON CONFLICT (topic, partition) DO UPDATE SET last_offset = EXCLUDED.last_offset"
        )
      ) {
        stmt.setString(1, record.topic());
        stmt.setInt(2, record.partition());
        stmt.setLong(3, record.offset());
        stmt.executeUpdate();
      } catch (SQLException e) {
        throw new RuntimeException("Failed to update offset in DB", e);
      }
    }

    @Override
    public ConsumerRebalanceListener createRebalanceListener() {
      return new ConsumerRebalanceListener() {
        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {}

        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
          try (
            final var conn = DriverManager.getConnection(jdbcUrl, username, password);
            final var stmt = conn.prepareStatement(
              "SELECT last_offset FROM consumer_offsets WHERE topic = ? AND partition = ?"
            )
          ) {
            for (TopicPartition tp : partitions) {
              stmt.setString(1, tp.topic());
              stmt.setInt(2, tp.partition());
              try (final var rs = stmt.executeQuery()) {
                if (rs.next()) {
                  long lastProcessed = rs.getLong("last_offset");
                  consumer.seek(tp, lastProcessed + 1);
                }
              }
            }
          } catch (SQLException e) {
            throw new RuntimeException("Failed to load offsets from DB", e);
          }
        }
      };
    }

    @Override
    public void notifyCommitComplete(final String id, final boolean success) {}

    @Override
    public OffsetState getState() {
      return state;
    }

    @Override
    public boolean isRunning() {
      return state == OffsetState.RUNNING;
    }

    @Override
    public Map<String, Object> getStatistics() {
      return Map.of();
    }

    @Override
    public void close() {
      stop();
    }

    // Test Helpers
    public void setOffsetInDb(final TopicPartition tp, final long offset) throws SQLException {
      try (
        final var conn = DriverManager.getConnection(jdbcUrl, username, password);
        final var stmt = conn.prepareStatement(
          "INSERT INTO consumer_offsets (topic, partition, last_offset) VALUES (?, ?, ?)"
        )
      ) {
        stmt.setString(1, tp.topic());
        stmt.setInt(2, tp.partition());
        stmt.setLong(3, offset);
        stmt.executeUpdate();
      }
    }

    public Long getOffsetFromDb(final TopicPartition tp) throws SQLException {
      try (
        final var conn = DriverManager.getConnection(jdbcUrl, username, password);
        final var stmt = conn.prepareStatement(
          "SELECT last_offset FROM consumer_offsets WHERE topic = ? AND partition = ?"
        )
      ) {
        stmt.setString(1, tp.topic());
        stmt.setInt(2, tp.partition());
        try (final var rs = stmt.executeQuery()) {
          if (rs.next()) {
            return rs.getLong("last_offset");
          }
        }
      }
      return null;
    }
  }

  @Test
  void shouldResumeFromPostgresOffset() throws InterruptedException, SQLException {
    // 1. Setup Mock Kafka Consumer
    final var mc = new MockConsumer<String, String>("earliest");
    mc.updateBeginningOffsets(Map.of(PARTITION, 0L));

    // 2. Pre-populate DB with an existing offset (e.g., we already finished up to offset 4)
    final var dbManager = new PostgresOffsetManager<>(
      mc,
      postgres.getJdbcUrl(),
      postgres.getUsername(),
      postgres.getPassword()
    );
    dbManager.setOffsetInDb(PARTITION, 4L);

    final var latch = new CountDownLatch(5); // Expecting 5, 6, 7, 8, 9

    // 4. Build KPipeConsumer with the custom DB Offset Manager
    final var consumer = KPipeConsumer.<String, String>builder()
      .withProperties(properties)
      .withTopic(TOPIC)
      .withConsumer(() -> mc)
      .withOffsetManager(dbManager)
      .withPipeline(val -> val)
      .withSequentialProcessing(true)
      .withMessageSink(_ -> {
        latch.countDown();
      })
      .build();

    // 5. Start Consumer
    consumer.start();

    // Trigger partition assignment manually for MockConsumer
    mc.rebalance(List.of(PARTITION));

    // 3. Populate Kafka with messages 0-9
    for (int i = 0; i < 10; i++) {
      mc.addRecord(new ConsumerRecord<>(TOPIC, 0, i, "k" + i, "v" + i));
    }

    // 6. Verify processing starts from offset 5
    assertTrue(latch.await(20, TimeUnit.SECONDS), "Consumer should process 5 remaining messages.");

    // Wait for the last offset to be marked as processed in the DB (polling for stability)
    long lastDbOffset = -1;
    for (int i = 0; i < 20; i++) {
      final var offset = dbManager.getOffsetFromDb(PARTITION);
      if (offset != null && offset == 9L) {
        lastDbOffset = offset;
        break;
      }
      Thread.sleep(100);
    }

    assertEquals(9L, lastDbOffset, "DB should be updated to offset 9. Actual: %d".formatted(lastDbOffset));

    consumer.close();
  }
}
