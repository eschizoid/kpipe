package io.github.eschizoid.kpipe.offsetstore;

import java.net.URI;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeDefinition;
import software.amazon.awssdk.services.dynamodb.model.BillingMode;
import software.amazon.awssdk.services.dynamodb.model.KeySchemaElement;
import software.amazon.awssdk.services.dynamodb.model.KeyType;
import software.amazon.awssdk.services.dynamodb.model.ScalarAttributeType;

/// DynamoDB Local in a container, and the offset table [DynamoDbOffsetStore] expects.
final class DynamoDbLocal {

  static final String TABLE = "kpipe-offsets";
  private static final DockerImageName IMAGE = DockerImageName.parse("amazon/dynamodb-local:3.3.2");
  private static final int PORT = 8000;

  private DynamoDbLocal() {}

  static GenericContainer<?> container() {
    return new GenericContainer<>(IMAGE).withExposedPorts(PORT);
  }

  static DynamoDbClient client(final GenericContainer<?> container) {
    return DynamoDbClient.builder()
      .endpointOverride(URI.create("http://%s:%d".formatted(container.getHost(), container.getMappedPort(PORT))))
      .region(Region.US_EAST_1)
      .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("local", "local")))
      .build();
  }

  /// Creates the offset table unless it already exists.
  static void createTable(final DynamoDbClient client) {
    if (client.listTables().tableNames().contains(TABLE)) return;
    client.createTable(request ->
      request
        .tableName(TABLE)
        .keySchema(KeySchemaElement.builder().attributeName(DynamoDbOffsetStore.KEY).keyType(KeyType.HASH).build())
        .attributeDefinitions(
          AttributeDefinition.builder()
            .attributeName(DynamoDbOffsetStore.KEY)
            .attributeType(ScalarAttributeType.S)
            .build()
        )
        .billingMode(BillingMode.PAY_PER_REQUEST)
    );
  }
}
