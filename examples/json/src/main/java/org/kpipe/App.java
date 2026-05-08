package org.kpipe;

import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import org.kpipe.consumer.config.AppConfig;
import org.kpipe.consumer.config.KafkaConsumerConfig;

/// Minimal JSON consumer demonstrating the KPipe facade. Reads `AppConfig` from environment,
/// builds a Kafka properties bundle via `KafkaConsumerConfig`, and starts a `KPipe.json(...)`
/// stream that logs every payload to the console.
public final class App {

  private static final Logger LOGGER = System.getLogger(App.class.getName());

  private App() {}

  static void main() {
    final var config = AppConfig.fromEnv();
    final var props = KafkaConsumerConfig.createConsumerConfig(config.bootstrapServers(), config.consumerGroup());

    try (final var handle = KPipe.json(config.topic(), props).toConsole().start()) {
      LOGGER.log(Level.INFO, "JSON consumer started for topic {0}", config.topic());
      handle.awaitShutdown();
    } catch (final Exception e) {
      LOGGER.log(Level.ERROR, "Fatal error in JSON consumer", e);
      System.exit(1);
    }
  }
}
