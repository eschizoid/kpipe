package org.kpipe.sink;

import static org.junit.jupiter.api.Assertions.*;

import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.List;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;

@Testcontainers(disabledWithoutDocker = true)
class CompositeSinkIntegrationTest {

  @Container
  static PostgreSQLContainer postgres = new PostgreSQLContainer("postgres:18")
    .withDatabaseName("kpipe_test")
    .withUsername("test")
    .withPassword("test");

  @BeforeAll
  static void setup() {
    Assumptions.assumeTrue(isDockerAvailable(), "Docker is not available, skipping integration test");
  }

  private static boolean isDockerAvailable() {
    try {
      final var process = new ProcessBuilder("docker", "info").start();
      return process.waitFor() == 0;
    } catch (final Exception e) {
      return false;
    }
  }

  @Test
  void testCompositeSinkWithPostgresAndCapturingSink() throws Exception {
    // 1. Setup Postgres Table
    try (
      final var conn = DriverManager.getConnection(
        postgres.getJdbcUrl(),
        postgres.getUsername(),
        postgres.getPassword()
      );
      final var stmt = conn.createStatement()
    ) {
      stmt.execute("CREATE TABLE processed_messages (id VARCHAR(255) PRIMARY KEY, content TEXT)");
    }

    // 2. Define the Postgres Sink
    final MessageSink<String, String> postgresSink = (record, value) -> {
      try (
        final var conn = DriverManager.getConnection(
          postgres.getJdbcUrl(),
          postgres.getUsername(),
          postgres.getPassword()
        );
        final var pstmt = conn.prepareStatement("INSERT INTO processed_messages (id, content) VALUES (?, ?)")
      ) {
        pstmt.setString(1, record.key());
        pstmt.setString(2, value);
        pstmt.executeUpdate();
      } catch (final Exception e) {
        throw new RuntimeException(e);
      }
    };

    // 3. Define a Capturing Sink for verification
    final var capturingSink = new MessageSink<String, String>() {
      private final List<String> values = new ArrayList<>();
      private final List<ConsumerRecord<String, String>> records = new ArrayList<>();

      @Override
      public void send(final ConsumerRecord<String, String> record, final String processedValue) {
        records.add(record);
        values.add(processedValue);
      }

      public List<String> getValues() {
        return values;
      }

      public List<ConsumerRecord<String, String>> getRecords() {
        return records;
      }
    };

    // 4. Create the Composite Sink
    final var compositeSink = new CompositeMessageSink<>(List.of(postgresSink, capturingSink));

    // 5. Execute
    final var record = new ConsumerRecord<>("test-topic", 0, 0, "msg-123", "original-content");
    compositeSink.send(record, "processed-content");

    // 6. Verify Capturing Sink
    assertEquals(1, capturingSink.getRecords().size());
    assertEquals("processed-content", capturingSink.getValues().getFirst());

    // 7. Verify Database
    try (
      final var conn = DriverManager.getConnection(
        postgres.getJdbcUrl(),
        postgres.getUsername(),
        postgres.getPassword()
      );
      final var stmt = conn.createStatement();
      final var rs = stmt.executeQuery("SELECT content FROM processed_messages WHERE id = 'msg-123'")
    ) {
      assertTrue(rs.next());
      assertEquals("processed-content", rs.getString("content"));
    }
  }
}
