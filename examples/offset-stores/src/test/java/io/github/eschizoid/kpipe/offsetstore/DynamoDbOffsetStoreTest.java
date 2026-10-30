package io.github.eschizoid.kpipe.offsetstore;

import io.github.eschizoid.kpipe.consumer.OffsetStore;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

@Testcontainers(disabledWithoutDocker = true)
class DynamoDbOffsetStoreTest extends OffsetStoreContract {

  @Container
  private static final GenericContainer<?> dynamo = DynamoDbLocal.container();

  private static DynamoDbClient client;

  @BeforeAll
  static void createTable() {
    client = DynamoDbLocal.client(dynamo);
    DynamoDbLocal.createTable(client);
  }

  @AfterAll
  static void closeClient() {
    client.close();
  }

  @Override
  OffsetStore store(final String group) {
    return new DynamoDbOffsetStore(client, DynamoDbLocal.TABLE, group);
  }
}
