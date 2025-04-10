package com.example.kafka;

import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

public class KafkaConsumerApp {

  private static final Logger LOGGER = System.getLogger(KafkaConsumerApp.class.getName());
  private static final CountDownLatch SHUTDOWN_LATCH = new CountDownLatch(1);
  private static final AtomicLong MESSAGES_PROCESSED = new AtomicLong(0);
  private static final String DEFAULT_BOOTSTRAP_SERVERS = "localhost:9092";
  private static final String DEFAULT_CONSUMER_GROUP = "functional-group";
  private static final String DEFAULT_TOPIC = "json-topic";
  private static final Duration DEFAULT_POLL_TIMEOUT = Duration.ofMillis(100);

  public static void main(String[] args) {
    final var bootstrapServers = getEnvOrDefault("KAFKA_BOOTSTRAP_SERVERS", DEFAULT_BOOTSTRAP_SERVERS);
    final var consumerGroup = getEnvOrDefault("KAFKA_CONSUMER_GROUP", DEFAULT_CONSUMER_GROUP);
    final var topic = getEnvOrDefault("KAFKA_TOPIC", DEFAULT_TOPIC);

    try {
      final var app = new KafkaConsumerApp(bootstrapServers, consumerGroup, topic);
      app.start();
      SHUTDOWN_LATCH.await();
    } catch (Exception e) {
      LOGGER.log(Level.ERROR, "Fatal error in Kafka consumer application", e);
      System.exit(1);
    }
  }

  private final FunctionalKafkaConsumer<byte[], byte[]> consumer;
  private final MessageProcessorRegistry registry;

  public KafkaConsumerApp(final String bootstrapServers, final String consumerGroup, String topic) {
    final var kafkaProps = KafkaConfigFactory.createConsumerConfig(bootstrapServers, consumerGroup);

    // Initialize the registry with application name
    this.registry = new MessageProcessorRegistry(getEnvOrDefault("APP_NAME", "kafka-consumer-app"));

    // Create message processor with metrics tracking
    final var processorPipeline = createMessageProcessorPipeline();

    // Create the consumer
    this.consumer =
      new FunctionalKafkaConsumer<>(kafkaProps, topic, wrapWithMetrics(processorPipeline), DEFAULT_POLL_TIMEOUT);
  }

  private Function<byte[], byte[]> createMessageProcessorPipeline() {
    // Use the registry instance to create the pipeline
    return registry.pipeline("parseJson", "addSource", "markProcessed", "addTimestamp");
  }

  private Function<byte[], byte[]> wrapWithMetrics(Function<byte[], byte[]> processor) {
    return message -> {
      final var result = processor.apply(message);
      MESSAGES_PROCESSED.incrementAndGet();
      return result;
    };
  }

  public void start() {
    // Register shutdown hooks
    registerShutdownHandlers();

    // Start metrics reporting
    startMetricsReporting();

    // Start the consumer
    consumer.start();

    LOGGER.log(Level.INFO, "Kafka consumer application started successfully");
  }

  private void registerShutdownHandlers() {
    Runtime
      .getRuntime()
      .addShutdownHook(
        new Thread(() -> {
          LOGGER.log(Level.INFO, "Shutdown signal received, closing resources...");
          try {
            consumer.close();
            LOGGER.log(Level.INFO, "Resources closed successfully");
          } catch (Exception e) {
            LOGGER.log(Level.WARNING, "Error during shutdown", e);
          } finally {
            SHUTDOWN_LATCH.countDown();
          }
        })
      );
  }

  private void startMetricsReporting() {
    Thread.startVirtualThread(() -> {
      try {
        while (consumer.isRunning()) {
          LOGGER.log(Level.INFO, "Messages processed: %d".formatted(MESSAGES_PROCESSED.get()));

          // Add registry metrics reporting
          registry
            .getAll()
            .keySet()
            .forEach(processorName -> {
              var metrics = registry.getMetrics(processorName);
              if (!metrics.isEmpty()) {
                LOGGER.log(Level.INFO, "Processor '%s' metrics: %s".formatted(processorName, metrics));
              }
            });

          Thread.sleep(Duration.ofMinutes(1));
        }
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    });
  }

  private static String getEnvOrDefault(final String name, final String defaultValue) {
    final var value = System.getenv(name);
    return value != null && !value.isEmpty() ? value : defaultValue;
  }
}
