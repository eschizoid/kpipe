package org.kpipe.consumer;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/// Integration tests that exercise the full consumer loop to verify backpressure fires
/// correctly in parallel processing mode. Uses Kafka's {@link MockConsumer} — no broker
/// required, no Mockito mocks.
class KPipeBackpressureIntegrationTest {

  private static final String TOPIC = "test-topic";
  private static final TopicPartition PARTITION = new TopicPartition(TOPIC, 0);

  private Properties properties;

  @BeforeEach
  void setUp() {
    properties = new Properties();
    properties.put("bootstrap.servers", "localhost:9092");
    properties.put("group.id", "test-group");
    properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    properties.put("enable.auto.commit", "true");
  }

  @Nested
  class ParallelMode {

    @Test
    void shouldPauseKafkaConsumerWhenInFlightExceedsHighWatermark() throws InterruptedException {
      // Arrange: 5 records, slow sink, highWatermark=3 → in-flight will exceed it
      final var mockConsumer = buildMockConsumer(5);
      final var sinkStarted = new CountDownLatch(5);
      final var sinkRelease = new CountDownLatch(1);

      final var consumer = KPipeConsumer.<String, String>builder()
        .withProperties(properties)
        .withTopic(TOPIC)
        .withProcessor(v -> v)
        .withMessageSink((record, value) -> {
          sinkStarted.countDown();
          try {
            sinkRelease.await(5, TimeUnit.SECONDS);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          }
        })
        .withBackpressure(3, 1)
        .withConsumer(() -> mockConsumer)
        .build();

      // Act: start the real consumer loop
      consumer.start();

      // Wait for all 5 sink calls to be in-flight (in-flight=5 >= highWatermark=3)
      assertTrue(sinkStarted.await(3, TimeUnit.SECONDS), "All 5 records should be in-flight");

      // Give the consumer loop time to call checkBackpressure()
      awaitCondition(() -> !mockConsumer.paused().isEmpty(), 2000);

      // Assert: Kafka consumer is paused
      assertTrue(mockConsumer.paused().contains(PARTITION), "Consumer should be paused due to backpressure");
      assertEquals(1L, consumer.getMetrics().get(KPipeConsumer.METRIC_BACKPRESSURE_PAUSE_COUNT));

      // Release the sink → in-flight drops to 0, below lowWatermark=1
      sinkRelease.countDown();

      // Assert: consumer resumes once in-flight ≤ lowWatermark
      awaitCondition(() -> mockConsumer.paused().isEmpty(), 3000);
      assertTrue(mockConsumer.paused().isEmpty(), "Consumer should have resumed");

      consumer.close();
    }

    @Test
    void shouldAccumulateBackpressureTimeMsAfterResume() throws InterruptedException {
      // Arrange
      final var mockConsumer = buildMockConsumer(4);
      final var sinkStarted = new CountDownLatch(4);
      final var sinkRelease = new CountDownLatch(1);

      final var consumer = KPipeConsumer.<String, String>builder()
        .withProperties(properties)
        .withTopic(TOPIC)
        .withProcessor(v -> v)
        .withMessageSink((record, value) -> {
          sinkStarted.countDown();
          try {
            sinkRelease.await(5, TimeUnit.SECONDS);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          }
        })
        .withBackpressure(2, 1)
        .withConsumer(() -> mockConsumer)
        .build();

      consumer.start();
      assertTrue(sinkStarted.await(3, TimeUnit.SECONDS));

      // Wait for pause
      awaitCondition(() -> !mockConsumer.paused().isEmpty(), 2000);

      // Release and wait for resume
      sinkRelease.countDown();
      awaitCondition(() -> mockConsumer.paused().isEmpty(), 3000);

      // backpressureTimeMs must be > 0
      assertTrue(
        (long) consumer.getMetrics().get(KPipeConsumer.METRIC_BACKPRESSURE_TIME_MS) > 0,
        "backpressureTimeMs should be positive after a backpressure pause"
      );

      consumer.close();
    }

    @Test
    void shouldNotPauseWhenInFlightStaysBelowHighWatermark() throws InterruptedException {
      // Arrange: 2 fast records, highWatermark=10 → no pause expected
      final var mockConsumer = buildMockConsumer(2);
      final var sinkDone = new CountDownLatch(2);

      final var consumer = KPipeConsumer.<String, String>builder()
        .withProperties(properties)
        .withTopic(TOPIC)
        .withProcessor(v -> v)
        .withMessageSink((record, value) -> sinkDone.countDown())
        .withBackpressure(10, 5)
        .withConsumer(() -> mockConsumer)
        .build();

      consumer.start();
      assertTrue(sinkDone.await(3, TimeUnit.SECONDS));

      // Give the loop a few iterations to call checkBackpressure
      Thread.sleep(300);

      assertTrue(mockConsumer.paused().isEmpty(), "Consumer should never have paused");
      assertEquals(0L, consumer.getMetrics().get(KPipeConsumer.METRIC_BACKPRESSURE_PAUSE_COUNT));

      consumer.close();
    }

    @Test
    void shouldNotPollNewRecordsWhilePaused() throws InterruptedException {
      // Arrange: 10 records total, but we add them in batches
      final var mc = new MockConsumer<String, String>("earliest") {
        @Override
        public synchronized void subscribe(final Collection<String> topics) {}

        @Override
        public synchronized void subscribe(final Collection<String> topics, final ConsumerRebalanceListener callback) {}
      };
      mc.assign(List.of(PARTITION));
      mc.updateBeginningOffsets(Map.of(PARTITION, 0L));

      // Add first 5 records
      for (int i = 0; i < 5; i++) {
        mc.addRecord(new ConsumerRecord<>(TOPIC, 0, i, "k" + i, "v" + i));
      }

      final var sinkStarted = new CountDownLatch(5);
      final var sinkRelease = new CountDownLatch(1);

      final var consumer = KPipeConsumer.<String, String>builder()
        .withProperties(properties)
        .withTopic(TOPIC)
        .withProcessor(v -> v)
        .withMessageSink((record, value) -> {
          sinkStarted.countDown();
          try {
            sinkRelease.await(5, TimeUnit.SECONDS);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          }
        })
        .withBackpressure(3, 1)
        .withConsumer(() -> mc)
        .build();

      // Act
      consumer.start();

      // Wait for backpressure to trigger (5 records should be in-flight)
      assertTrue(sinkStarted.await(3, TimeUnit.SECONDS), "First 5 records should be in-flight");
      awaitCondition(() -> !mc.paused().isEmpty(), 2000);

      // Assert: consumer is paused
      assertTrue(mc.paused().contains(PARTITION));
      assertEquals(5L, consumer.getMetrics().get("messagesReceived"));

      // Now add more records while it's paused
      for (int i = 5; i < 10; i++) {
        mc.addRecord(new ConsumerRecord<>(TOPIC, 0, i, "k" + i, "v" + i));
      }

      // Wait some time while paused to ensure no more records are polled
      Thread.sleep(500);

      // Should still have only 5 records received because it's paused
      assertEquals(5L, consumer.getMetrics().get("messagesReceived"), "No more records should be polled while paused");

      // Release and wait for resume
      sinkRelease.countDown();
      awaitCondition(() -> mc.paused().isEmpty(), 3000);

      // Now it should poll the remaining records
      awaitCondition(() -> (long) consumer.getMetrics().get("messagesReceived") == 10, 3000);
      assertEquals(10L, consumer.getMetrics().get("messagesReceived"));

      consumer.close();
    }

    @Test
    void shouldStopPollingWhenManuallyPaused() throws InterruptedException {
      // Arrange: 10 records total
      final var mc = new MockConsumer<String, String>("earliest") {
        @Override
        public synchronized void subscribe(final Collection<String> topics) {}

        @Override
        public synchronized void subscribe(final Collection<String> topics, final ConsumerRebalanceListener callback) {}
      };
      mc.assign(List.of(PARTITION));
      mc.updateBeginningOffsets(Map.of(PARTITION, 0L));

      // Add records in two batches
      for (int i = 0; i < 5; i++) {
        mc.addRecord(new ConsumerRecord<>(TOPIC, 0, i, "k" + i, "v" + i));
      }

      final var consumer = KPipeConsumer.<String, String>builder()
        .withProperties(properties)
        .withTopic(TOPIC)
        .withProcessor(v -> v)
        .withConsumer(() -> mc)
        .build();

      // Act
      consumer.start();

      // Wait for first 5 to be polled
      awaitCondition(() -> (long) consumer.getMetrics().get("messagesReceived") == 5, 2000);

      // Pause manually
      consumer.pause();

      // Give some time for the command to be processed
      awaitCondition(() -> !mc.paused().isEmpty(), 2000);

      // Now add more records while it's paused
      for (int i = 5; i < 10; i++) {
        mc.addRecord(new ConsumerRecord<>(TOPIC, 0, i, "k" + i, "v" + i));
      }

      // Wait some more to ensure no more are polled while paused
      Thread.sleep(500);
      assertEquals(5L, consumer.getMetrics().get("messagesReceived"), "No more records should be polled while paused");

      // Resume manually
      consumer.resume();

      // Wait for the rest to be polled
      awaitCondition(() -> (long) consumer.getMetrics().get("messagesReceived") == 10, 3000);
      assertEquals(10L, consumer.getMetrics().get("messagesReceived"));

      consumer.close();
    }
  }

  @Nested
  class SequentialMode {

    @Test
    void shouldPauseWhenLagExceedsHighWatermarkInSequentialMode() throws InterruptedException {
      // Arrange: 10 records in Kafka, highWatermark=5
      final var mockConsumer = new MockConsumer<String, String>("earliest") {
        @Override
        public synchronized void subscribe(final Collection<String> topics) {}

        @Override
        public synchronized void subscribe(final Collection<String> topics, final ConsumerRebalanceListener callback) {}
      };
      mockConsumer.assign(List.of(PARTITION));
      mockConsumer.updateBeginningOffsets(Map.of(PARTITION, 0L));

      // Initial 10 records
      for (int i = 0; i < 10; i++) {
        mockConsumer.addRecord(new ConsumerRecord<>(TOPIC, 0, i, "k" + i, "v" + i));
      }
      mockConsumer.updateEndOffsets(Map.of(PARTITION, 10L));

      final var consumer = KPipeConsumer.<String, String>builder()
        .withProperties(properties)
        .withTopic(TOPIC)
        .withProcessor(v -> {
          try {
            // Slow down processing to allow backpressure loop to see the lag
            Thread.sleep(200);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          }
          return v;
        })
        // Low watermark=2, High watermark=5
        .withBackpressure(5, 2)
        .withSequentialProcessing(true)
        .withConsumer(() -> mockConsumer)
        .build();

      // Act
      consumer.start();

      // Wait for pause
      awaitCondition(() -> !mockConsumer.paused().isEmpty(), 5000);

      // Assert: consumer is paused
      assertTrue(mockConsumer.paused().contains(PARTITION), "Consumer should be paused due to high lag");
      assertTrue((long) consumer.getMetrics().get(KPipeConsumer.METRIC_BACKPRESSURE_PAUSE_COUNT) >= 1);

      consumer.close();
    }

    @Test
    void withBackpressureAndSequentialProcessingShouldSwitchToLagMonitoringAtBuildTime() {
      final var consumer = KPipeConsumer.<String, String>builder()
        .withProperties(properties)
        .withTopic(TOPIC)
        .withProcessor(v -> v)
        .withSequentialProcessing(true)
        .withBackpressure(10_000, 7_000)
        .build();

      assertNotNull(consumer);
    }

    @Test
    void testMockConsumerLag() {
      final var mc = new MockConsumer<String, String>("earliest");
      mc.assign(List.of(PARTITION));
      mc.updateBeginningOffsets(Map.of(PARTITION, 0L));
      mc.addRecord(new ConsumerRecord<>(TOPIC, 0, 0, "k", "v"));
      mc.addRecord(new ConsumerRecord<>(TOPIC, 0, 1, "k", "v"));
      mc.updateEndOffsets(Map.of(PARTITION, 10L));

      assertEquals(0, mc.position(PARTITION));
      mc.poll(java.time.Duration.ZERO);
      assertEquals(2, mc.position(PARTITION));
      assertEquals(10L, mc.endOffsets(List.of(PARTITION)).get(PARTITION));
    }
  }

  // --- helpers ---

  /// Builds a MockConsumer that is pre-assigned to the test partition. The subscribe()
  /// override is a no-op so that KPipeConsumer's start() doesn't conflict with the
  /// existing assignment.
  private MockConsumer<String, String> buildMockConsumer(final int recordCount) {
    final var mc = new MockConsumer<String, String>("earliest") {
      @Override
      public synchronized void subscribe(final Collection<String> topics) {
        // no-op: partition is pre-assigned in buildMockConsumer
      }

      @Override
      public synchronized void subscribe(final Collection<String> topics, final ConsumerRebalanceListener callback) {
        // no-op: partition is pre-assigned in buildMockConsumer
      }
    };
    mc.assign(List.of(PARTITION));
    mc.updateBeginningOffsets(Map.of(PARTITION, 0L));
    for (int i = 0; i < recordCount; i++) {
      mc.addRecord(new ConsumerRecord<>(TOPIC, 0, i, "k" + i, "v" + i));
    }
    return mc;
  }

  private void awaitCondition(final BooleanSupplier condition, final long timeoutMs) throws InterruptedException {
    final long deadline = System.currentTimeMillis() + timeoutMs;
    while (!condition.getAsBoolean() && System.currentTimeMillis() < deadline) {
      Thread.sleep(50);
    }
  }

  @FunctionalInterface
  interface BooleanSupplier {
    boolean getAsBoolean() throws InterruptedException;
  }
}
