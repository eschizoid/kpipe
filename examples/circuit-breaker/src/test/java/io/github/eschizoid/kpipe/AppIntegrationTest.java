package io.github.eschizoid.kpipe;

import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.eschizoid.kpipe.consumer.ProcessingMode;
import io.github.eschizoid.kpipe.producer.config.KafkaProducerConfig;
import io.github.eschizoid.kpipe.sink.MessageSink;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

@Testcontainers(disabledWithoutDocker = true)
class AppIntegrationTest {

  private static final String KAFKA_VERSION = System.getProperty("kafkaVersion", "4.2.0");

  @Container
  static KafkaContainer kafka = new KafkaContainer(
    DockerImageName.parse("soldevelo/kafka:%s".formatted(KAFKA_VERSION)).asCompatibleSubstituteFor("apache/kafka")
  ).withStartupAttempts(3);

  @Test
  void circuitBreakerTripsAndRecovers() throws Exception {
    final var topic = "cb-topic-" + UUID.randomUUID().toString().substring(0, 8);
    final var processed = new AtomicInteger(0);
    final var FAIL_UNTIL = 10;

    final MessageSink<Map<String, Object>> flakySink = msg -> {
      final var n = processed.incrementAndGet();
      if (n <= FAIL_UNTIL) throw new RuntimeException("simulated failure #" + n);
    };

    try (
      final var handle = KPipe.json(topic, consumerProps())
        .withProcessingMode(ProcessingMode.SEQUENTIAL)
        .withCircuitBreaker(0.5, 5, Duration.ofMillis(300))
        .toCustom(flakySink)
        .start()
    ) {
      produceRecords(topic, 20);

      // Wait for the full trip-and-probe cycle to have completed at least once. timeOpenMs only
      // increments inside the OPEN → HALF_OPEN transition, so a non-zero value proves the probe
      // timer fired (not just the trip).
      final var deadline = System.currentTimeMillis() + 15_000;
      while (System.currentTimeMillis() < deadline) {
        final var trips = (long) handle.metrics().get("circuitBreakerTrips");
        final var timeOpenMs = (long) handle.metrics().get("circuitBreakerTimeOpenMs");
        if (trips >= 1 && timeOpenMs > 0L) break;
        Thread.sleep(100);
      }

      final var trips = (long) handle.metrics().get("circuitBreakerTrips");
      final var timeOpenMs = (long) handle.metrics().get("circuitBreakerTimeOpenMs");

      assertTrue(trips >= 1, "breaker should have tripped at least once, got " + trips);
      assertTrue(timeOpenMs > 0L, "circuitBreakerTimeOpenMs should be > 0 after the probe fired, got " + timeOpenMs);

      assertTrue(handle.shutdownGracefully(Duration.ofSeconds(5)));
    }
  }

  private static Properties consumerProps() {
    final var props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "cb-test-" + UUID.randomUUID());
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    return props;
  }

  private static void produceRecords(final String topic, final int count) throws Exception {
    final var producerProps = KafkaProducerConfig.createProducerConfig(kafka.getBootstrapServers());
    try (final var producer = new KafkaProducer<byte[], byte[]>(producerProps)) {
      for (int i = 0; i < count; i++) {
        final var payload = """
          {"id":%d,"event":"e%d"}""".formatted(i, i)
          .getBytes(StandardCharsets.UTF_8);
        producer.send(new ProducerRecord<>(topic, payload)).get();
      }
    }
  }
}
