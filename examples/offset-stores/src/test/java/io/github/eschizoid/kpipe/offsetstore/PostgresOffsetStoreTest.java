package io.github.eschizoid.kpipe.offsetstore;

import static org.junit.jupiter.api.Assertions.assertThrows;

import io.github.eschizoid.kpipe.consumer.OffsetStore;
import java.sql.SQLException;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;

@Testcontainers(disabledWithoutDocker = true)
class PostgresOffsetStoreTest extends OffsetStoreContract {

  private static final String TABLE = "kpipe_offsets";

  @Container
  private static final PostgreSQLContainer postgres = new PostgreSQLContainer("postgres:16-alpine");

  private static PGSimpleDataSource dataSource;

  @BeforeAll
  static void createTable() throws SQLException {
    dataSource = new PGSimpleDataSource();
    dataSource.setUrl(postgres.getJdbcUrl());
    dataSource.setUser(postgres.getUsername());
    dataSource.setPassword(postgres.getPassword());
    try (final var connection = dataSource.getConnection(); final var statement = connection.createStatement()) {
      statement.execute(
        "CREATE TABLE " +
          TABLE +
          " (group_id TEXT, topic TEXT, partition INT, next_offset BIGINT, PRIMARY KEY (group_id, topic, partition))"
      );
    }
  }

  @Override
  OffsetStore store(final String group) {
    return new PostgresOffsetStore(dataSource, TABLE, group);
  }

  @Test
  void aTableNameThatIsNotAPlainIdentifierIsRejected() {
    assertThrows(IllegalArgumentException.class, () ->
      new PostgresOffsetStore(dataSource, "offsets; DROP TABLE users", "payments")
    );
  }
}
