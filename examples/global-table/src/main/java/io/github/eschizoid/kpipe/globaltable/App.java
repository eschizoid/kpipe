package io.github.eschizoid.kpipe.globaltable;

import io.github.eschizoid.kpipe.KPipe;
import io.github.eschizoid.kpipe.consumer.ProcessingMode;
import io.github.eschizoid.kpipe.consumer.config.KafkaConsumerConfig;
import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.util.Optional;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;

/// A consumer that keeps its offsets in this region's replica of a DynamoDB global table. Run the
/// same app in every region, each against its own Kafka cluster and its own `AWS_REGION`; when one
/// region goes down, the group in another region resumes from the offsets that replicated there.
public final class App {

  private static final Logger LOGGER = System.getLogger(App.class.getName());

  private App() {}

  static void main() {
    final var bootstrapServers = env("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092");
    final var group = env("KAFKA_CONSUMER_GROUP", "payments");
    final var topic = env("KAFKA_TOPIC", "payments");
    final var table = env("OFFSET_TABLE", "kpipe-offsets");
    final var region = Region.of(env("AWS_REGION", "us-east-1"));

    final var props = KafkaConsumerConfig.createConsumerConfig(bootstrapServers, group);
    try (
      final var dynamo = DynamoDbClient.builder().region(region).build();
      final var handle = KPipe.bytes(topic, props)
        .withProcessingMode(ProcessingMode.PARALLEL)
        .withOffsetStore(new DynamoDbOffsetStore(dynamo, table, group))
        .toCustom(payload -> LOGGER.log(Level.INFO, "processed {0} bytes", payload.length))
        .start()
    ) {
      LOGGER.log(Level.INFO, "Consuming {0} in {1}, offsets in table {2}", topic, region, table);
      handle.awaitShutdown();
    } catch (final Exception e) {
      LOGGER.log(Level.ERROR, "Fatal error in global-table consumer", e);
      System.exit(1);
    }
  }

  private static String env(final String name, final String fallback) {
    return Optional.ofNullable(System.getenv(name)).orElse(fallback);
  }
}
