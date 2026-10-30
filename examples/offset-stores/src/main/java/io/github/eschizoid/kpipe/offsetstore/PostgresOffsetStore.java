package io.github.eschizoid.kpipe.offsetstore;

import io.github.eschizoid.kpipe.consumer.OffsetStore;
import java.sql.SQLException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Pattern;
import javax.sql.DataSource;
import org.apache.kafka.common.TopicPartition;

/// An [OffsetStore] backed by a Postgres table.
///
/// The table needs this shape; each row holds one partition's next offset to read:
///
/// ```sql
/// CREATE TABLE kpipe_offsets (
///   group_id TEXT, topic TEXT, partition INT, next_offset BIGINT,
///   PRIMARY KEY (group_id, topic, partition))
/// ```
///
/// A commit writes every partition in one transaction, so a failed commit leaves every partition
/// at its previous offset and the next commit carries the then-current frontier.
public final class PostgresOffsetStore implements OffsetStore {

  private static final Pattern IDENTIFIER = Pattern.compile("[A-Za-z_][A-Za-z0-9_]*");

  private final DataSource dataSource;
  private final String group;
  private final String select;
  private final String upsert;

  /// @param dataSource where to open connections to the database
  /// @param table the table name, a plain SQL identifier
  /// @param group the consumer group whose offsets this store keeps
  /// @throws IllegalArgumentException if `table` is not a plain SQL identifier
  public PostgresOffsetStore(final DataSource dataSource, final String table, final String group) {
    this.dataSource = Objects.requireNonNull(dataSource, "dataSource");
    this.group = Objects.requireNonNull(group, "group");
    if (!IDENTIFIER.matcher(Objects.requireNonNull(table, "table")).matches()) {
      throw new IllegalArgumentException("table must be a plain SQL identifier, got " + table);
    }
    this.select = "SELECT next_offset FROM %s WHERE group_id = ? AND topic = ? AND partition = ?".formatted(table);
    this.upsert =
      "INSERT INTO %s (group_id, topic, partition, next_offset) VALUES (?, ?, ?, ?) ".formatted(table) +
      "ON CONFLICT (group_id, topic, partition) DO UPDATE SET next_offset = EXCLUDED.next_offset";
  }

  @Override
  public Map<TopicPartition, Long> load(final Collection<TopicPartition> partitions) {
    final var offsets = new HashMap<TopicPartition, Long>();
    try (final var connection = dataSource.getConnection(); final var statement = connection.prepareStatement(select)) {
      for (final var partition : partitions) {
        statement.setString(1, group);
        statement.setString(2, partition.topic());
        statement.setInt(3, partition.partition());
        try (final var rows = statement.executeQuery()) {
          if (rows.next()) offsets.put(partition, rows.getLong(1));
        }
      }
      return offsets;
    } catch (final SQLException e) {
      throw new IllegalStateException("Failed to load offsets for " + partitions, e);
    }
  }

  @Override
  public void commit(final Map<TopicPartition, Long> offsets) {
    try (final var connection = dataSource.getConnection(); final var statement = connection.prepareStatement(upsert)) {
      final var autoCommit = connection.getAutoCommit();
      connection.setAutoCommit(false);
      try {
        for (final var entry : offsets.entrySet()) {
          statement.setString(1, group);
          statement.setString(2, entry.getKey().topic());
          statement.setInt(3, entry.getKey().partition());
          statement.setLong(4, entry.getValue());
          statement.addBatch();
        }
        statement.executeBatch();
        connection.commit();
      } catch (final SQLException | RuntimeException e) {
        // JDBC leaves an open transaction's fate on close to the driver or pool.
        try {
          connection.rollback();
        } catch (final SQLException rollback) {
          e.addSuppressed(rollback);
        }
        throw e;
      } finally {
        connection.setAutoCommit(autoCommit);
      }
    } catch (final SQLException e) {
      throw new IllegalStateException("Failed to commit offsets " + offsets, e);
    }
  }
}
