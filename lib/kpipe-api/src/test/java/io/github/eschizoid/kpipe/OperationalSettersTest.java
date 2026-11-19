package io.github.eschizoid.kpipe;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.eschizoid.kpipe.consumer.ConsumerSettings;
import io.github.eschizoid.kpipe.consumer.OffsetStore;
import java.time.Duration;
import java.util.Collection;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.logging.Handler;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.junit.jupiter.api.Test;

/// Pins the operational settings that the fluent facade exposes alongside the explicit builder.
///
/// These configure how a consumer behaves in a deployment rather than how records are processed,
/// which is why they belong on the 80% path: reaching one used to mean abandoning `KPipe.json(...)`
/// and rebuilding through `KPipeConsumerBuilder` to obtain something orthogonal to the pipeline.
///
/// Every setter validates its value in [ConsumerSettings] when it is called, on the facade and the
/// builder alike, and `ConsumerSettingsParityTest` checks that each setting reaches the settings
/// on all three surfaces. These tests cover the values and the immutability of a stream.
class OperationalSettersTest {

  private static final OffsetStore STORE = new OffsetStore() {
    @Override
    public Map<TopicPartition, Long> load(final Collection<TopicPartition> partitions) {
      return Map.of();
    }

    @Override
    public void commit(final Map<TopicPartition, Long> offsets) {}
  };

  private static Properties props() {
    final var p = new Properties();
    p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
    p.put(ConsumerConfig.GROUP_ID_CONFIG, "g");
    p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
    p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
    return p;
  }

  private static ConsumerSettings configOf(final Stream<?> s) {
    return ((DefaultStream<?>) s).settings();
  }

  private static Stream<byte[]> stream() {
    return KPipe.bytes("t", props());
  }

  @Test
  void everySetterReachesTheConfig() {
    assertEquals(
      Duration.ofSeconds(30),
      configOf(stream().withMetricsLog(Duration.ofSeconds(30))).metricsLogInterval()
    );
    assertTrue(configOf(stream().withShutdownHook(true)).shutdownHook());
    assertEquals(
      Duration.ofSeconds(7),
      configOf(stream().withThreadTerminationTimeout(Duration.ofSeconds(7))).threadTerminationTimeout()
    );
    assertEquals(
      Duration.ofSeconds(9),
      configOf(stream().withWaitForMessagesTimeout(Duration.ofSeconds(9))).waitForMessagesTimeout()
    );
    assertEquals(Duration.ofSeconds(5), configOf(stream().withCommitInterval(Duration.ofSeconds(5))).commitInterval());
    assertSame(STORE, configOf(stream().withOffsetStore(STORE)).offsetStore());
  }

  @Test
  void anUntouchedStreamCarriesTheDefaultSettings() {
    final var c = configOf(stream());
    assertTrue(c.explicitlySet().isEmpty(), "no setting counts as set until its method is called");
    assertSame(ConsumerSettings.defaults(), c);
  }

  @Test
  void settersReturnNewStreamsAndLeaveTheOriginalAlone() {
    final var base = stream();
    final var derived = base.withShutdownHook(true).withMetricsLog(Duration.ofSeconds(5));
    assertFalse(configOf(base).shutdownHook(), "the original stream must not observe a later setter");
    assertNull(configOf(base).metricsLogInterval());
    assertTrue(configOf(derived).shutdownHook());
  }

  @Test
  void aNonPositiveMetricsLogIntervalIsRejectedAtTheCallSite() {
    final var zero = assertThrows(IllegalArgumentException.class, () -> stream().withMetricsLog(Duration.ZERO));
    assertTrue(zero.getMessage().contains("interval"), zero.getMessage());
    assertThrows(IllegalArgumentException.class, () -> stream().withMetricsLog(Duration.ofSeconds(-1)));
    assertThrows(IllegalArgumentException.class, () -> KPipe.multi(props()).withMetricsLog(Duration.ZERO));
  }

  @Test
  void nullArgumentsAreRejectedAtTheCallSite() {
    assertThrows(NullPointerException.class, () -> stream().withMetricsLog(null));
    assertThrows(NullPointerException.class, () -> stream().withThreadTerminationTimeout(null));
    assertThrows(NullPointerException.class, () -> stream().withWaitForMessagesTimeout(null));
    assertThrows(NullPointerException.class, () -> stream().withCommitInterval(null));
    assertThrows(NullPointerException.class, () -> stream().withOffsetStore(null));
  }

  @Test
  void invalidValuesAreRejectedWhenTheSetterIsCalled() {
    final var threadTermination = assertThrows(IllegalArgumentException.class, () ->
      stream().withThreadTerminationTimeout(Duration.ofSeconds(-1))
    );
    assertTrue(threadTermination.getMessage().contains("negative"), threadTermination.getMessage());

    final var waitForMessages = assertThrows(IllegalArgumentException.class, () ->
      stream().withWaitForMessagesTimeout(Duration.ofSeconds(-1))
    );
    assertTrue(waitForMessages.getMessage().contains("negative"), waitForMessages.getMessage());

    final var commitInterval = assertThrows(IllegalArgumentException.class, () ->
      stream().withCommitInterval(Duration.ZERO)
    );
    assertTrue(commitInterval.getMessage().contains("1ms"), commitInterval.getMessage());
  }

  @Test
  void theMetricsLogWritesALineEachIntervalAndNoneAfterClose() throws Exception {
    // Covers the setting end to end: facade setter, settings, build, start and the log thread. A
    // latch rather than a sleep so a missing delegation fails fast.
    final var lines = new CopyOnWriteArrayList<String>();
    final var threeLines = new CountDownLatch(3);
    final var logger = Logger.getLogger("io.github.eschizoid.kpipe.metrics.log");
    final var handler = new Handler() {
      @Override
      public void publish(final LogRecord record) {
        lines.add(record.getMessage());
        threeLines.countDown();
      }

      @Override
      public void flush() {}

      @Override
      public void close() {}
    };
    logger.addHandler(handler);
    try {
      try (
        final var handle = KPipe.bytes("t", props())
          .withMetricsLog(Duration.ofMillis(20))
          .toCustom(_ -> {})
          .start()
      ) {
        assertTrue(threeLines.await(5, TimeUnit.SECONDS), "a line every 20ms");
      }
      final var atClose = lines.size();
      Thread.sleep(100);
      assertEquals(atClose, lines.size(), "no line after close");
      assertTrue(lines.getFirst().startsWith("kpipe metrics [t group="), lines.getFirst());
      assertTrue(lines.getFirst().contains(" received=0 "), lines.getFirst());
    } finally {
      logger.removeHandler(handler);
    }
  }

  @Test
  void multiBuilderMirrorsAcceptTheSameSettings() {
    // The legitimate path for what the per-route test rejects. Not calling start() — that would
    // open a real Kafka connection; chaining proves the mirrors exist and validate.
    KPipe.multi(props())
      .withMetricsLog(Duration.ofSeconds(30))
      .withShutdownHook(true)
      .withThreadTerminationTimeout(Duration.ofSeconds(7))
      .withWaitForMessagesTimeout(Duration.ofSeconds(9))
      .withCommitInterval(Duration.ofSeconds(5))
      .withOffsetStore(STORE)
      .bytes("topic-a", s -> s.toCustom(_ -> {}));
  }
}
