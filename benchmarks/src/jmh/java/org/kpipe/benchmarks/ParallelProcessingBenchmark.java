package org.kpipe.benchmarks;

import io.confluent.parallelconsumer.ParallelConsumerOptions;
import io.confluent.parallelconsumer.ParallelStreamProcessor;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.kpipe.config.KafkaConsumerConfig;
import org.kpipe.consumer.FunctionalConsumer;
import org.openjdk.jmh.annotations.*;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

/// JMH Benchmark for comparing KPipe's parallel processing engine against the
/// Confluent Parallel Consumer.
///
/// This benchmark measures raw throughput (messages processed per second) by
/// simulating a high-concurrency message ingestion scenario. Both frameworks
/// are tested against a real Kafka instance (via Testcontainers) to ensure
/// realistic performance metrics.
///
/// ### Scenarios:
/// 1. **KPipe Parallel Mode**: Leverages Java 24 Virtual Threads (Project Loom) for
///    record-level parallelism with minimal overhead.
/// 2. **Confluent Parallel Consumer**: Industry-standard library for parallel
///    processing, using traditional platform thread pools.
///
/// ### Design Integrity:
/// - Both frameworks start from the beginning of the topic (`earliest`) for each iteration.
/// - Both process exactly **1,000 messages** per iteration.
/// - KPipe uses Loom; Confluent uses a max concurrency of **100**.
///
/// ### Running the Benchmark:
/// ```bash
/// ./gradlew :benchmarks:jmh -Pjmh.includes='ParallelProcessingBenchmark'
/// ```
@State(Scope.Benchmark)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
public class ParallelProcessingBenchmark {

  private static final String TOPIC = "benchmark-topic";
  @State(Scope.Benchmark)
  public static class DockerContext {
    private KafkaContainer kafka;

    @Setup(Level.Trial)
    public void setup() throws Exception {
      System.setProperty("DOCKER_API_VERSION", "1.44");
      System.setProperty("TESTCONTAINERS_RYUK_DISABLED", "true");
      final var imageName = DockerImageName
        .parse("confluentinc/cp-kafka:7.7.1")
        .asCompatibleSubstituteFor("apache/kafka");
      kafka = new KafkaContainer(imageName);
      kafka.start();

      final var bootstrapServers = kafka.getBootstrapServers();

      // Seed some data
      final var producerProps = new Properties();
      producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
      producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
      producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());

      try (final var producer = new KafkaProducer<byte[], byte[]>(producerProps)) {
        final var value =
          """
                  {
                    "id": 12345,
                    "message": "Benchmark message"
                  }
                  """.getBytes(
              StandardCharsets.UTF_8
            );

        for (int i = 0; i < 1000; i++) {
          producer.send(new ProducerRecord<>(TOPIC, value));
        }
        producer.flush();
      }
    }

    @TearDown(Level.Trial)
    public void tearDown() {
      if (kafka != null) kafka.stop();
    }

    public String getBootstrapServers() {
      return kafka.getBootstrapServers();
    }
  }

  @Benchmark
  public void kpipeParallelProcessing(final DockerContext context) throws Exception {
    final var currentProcessedCount = new AtomicInteger(0);
    final var bootstrapServers = context.getBootstrapServers();

    final var kpipeProps = KafkaConsumerConfig
      .consumerBuilder()
      .withBootstrapServers(bootstrapServers)
      .withGroupId("kpipe-group-" + UUID.randomUUID())
      .withByteArrayDeserializers()
      .withAutoCommit(false)
      .build();

    try (
      final var consumer = FunctionalConsumer
        .<byte[], byte[]>builder()
        .withProperties(kpipeProps)
        .withTopic(TOPIC)
        .withProcessor(val -> {
          currentProcessedCount.incrementAndGet();
          return val;
        })
        .withSequentialProcessing(false)
        .build()
    ) {
      consumer.start();

      while (currentProcessedCount.get() < 1000) {
        Thread.onSpinWait();
      }
    }
  }

  @Benchmark
  public void confluentParallelProcessing(final DockerContext context) throws Exception {
    final var currentProcessedCount = new AtomicInteger(0);
    final var bootstrapServers = context.getBootstrapServers();

    final var consumerProps = new Properties();
    consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "confluent-group-" + UUID.randomUUID());
    consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
    consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());

    try (
      final var kafkaConsumer = new KafkaConsumer<byte[], byte[]>(consumerProps);
      final var processor = ParallelStreamProcessor.createEosStreamProcessor(
        ParallelConsumerOptions
          .<byte[], byte[]>builder()
          .ordering(ParallelConsumerOptions.ProcessingOrder.UNORDERED)
          .maxConcurrency(100)
          .consumer(kafkaConsumer)
          .build()
      )
    ) {
      processor.subscribe(Collections.singletonList(TOPIC));
      processor.poll(record -> {
        currentProcessedCount.incrementAndGet();
      });

      while (currentProcessedCount.get() < 1000) {
        Thread.onSpinWait();
      }
    }
  }
}
