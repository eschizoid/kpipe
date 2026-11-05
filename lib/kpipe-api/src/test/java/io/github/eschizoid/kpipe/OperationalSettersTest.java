package io.github.eschizoid.kpipe;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.eschizoid.kpipe.consumer.OffsetStore;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.UnaryOperator;
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
/// **The `applyTo` delegation is covered, via the builder's own preconditions.** Passing an invalid
/// value through a facade setter reaches the builder's validation during `start()`, so a missing
/// delegation line means the value never arrives and nothing throws — which makes the assertion a
/// test of the delegation rather than of the setter. No broker, no reflection.
///
/// `withShutdownHook` is the exception: `true` and `false` are both valid, so there is no
/// precondition to trip and nothing observable to assert without reaching into the JVM's
/// shutdown-hook registry. Its delegation is covered by inspection alone.
///
/// `withOffsetStore` has no invalid value either. `GlobalTableFailoverIntegrationTest` in
/// `examples/offset-stores` covers its delegation: the consumer there writes to the store only
/// when the facade passes it on.
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

  private static io.github.eschizoid.kpipe.ConsumerConfig configOf(final Stream<?> s) {
    return ((DefaultStream<?>) s).consumerConfig();
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
    assertEquals(Boolean.TRUE, configOf(stream().withShutdownHook(true)).shutdownHook());
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
  void unsetLeavesNullSoTheBuilderKeepsItsOwnDefault() {
    // Null is the facade's "unset" encoding — applyTo skips it, so the builder's default stands.
    // A setter defaulting to a concrete value here would silently override that default for
    // everyone, including callers who never touched the setting.
    final var c = configOf(stream());
    assertNull(c.metricsLogInterval());
    assertNull(c.shutdownHook());
    assertNull(c.threadTerminationTimeout());
    assertNull(c.waitForMessagesTimeout());
    assertNull(c.commitInterval());
    assertNull(c.offsetStore());
  }

  @Test
  void settersReturnNewStreamsAndLeaveTheOriginalAlone() {
    final var base = stream();
    final var derived = base.withShutdownHook(true).withMetricsLog(Duration.ofSeconds(5));
    assertNull(configOf(base).shutdownHook(), "the original stream must not observe a later setter");
    assertNull(configOf(base).metricsLogInterval());
    assertEquals(Boolean.TRUE, configOf(derived).shutdownHook());
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
  void invalidValuesReachTheBuilderAndTripItsPreconditions() {
    // The seam: applyTo runs before build(), so a value the builder rejects surfaces here. If a
    // delegation line were missing the value would never arrive and nothing would throw, which is
    // exactly what makes this a test of the delegation rather than of the setter.
    final var threadTermination = assertThrows(IllegalArgumentException.class, () ->
      KPipe.bytes("t", props())
        .withThreadTerminationTimeout(Duration.ofSeconds(-1))
        .toCustom(_ -> {})
        .start()
    );
    assertTrue(threadTermination.getMessage().contains("negative"), threadTermination.getMessage());

    final var waitForMessages = assertThrows(IllegalArgumentException.class, () ->
      KPipe.bytes("t", props())
        .withWaitForMessagesTimeout(Duration.ofSeconds(-1))
        .toCustom(_ -> {})
        .start()
    );
    assertTrue(waitForMessages.getMessage().contains("negative"), waitForMessages.getMessage());

    final var commitInterval = assertThrows(IllegalArgumentException.class, () ->
      KPipe.bytes("t", props())
        .withCommitInterval(Duration.ZERO)
        .toCustom(_ -> {})
        .start()
    );
    assertTrue(commitInterval.getMessage().contains("1ms"), commitInterval.getMessage());
  }

  @Test
  void theMetricsLogWritesALineEachIntervalAndAFinalOneAtClose() throws Exception {
    // Covers the setting end to end: facade setter, config, applyTo, build, start and the log
    // thread. A latch rather than a sleep so a missing delegation fails fast.
    final var lines = new CopyOnWriteArrayList<String>();
    final var threeLines = new CountDownLatch(3);
    final var logger = Logger.getLogger("io.github.eschizoid.kpipe.metrics");
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
  void multiBuilderRejectsThemPerRouteAndNamesItsOwnMirror() {
    // Registering each in CONSUMER_WIDE_SETTINGS is what makes a route-level call an error rather
    // than a silent no-op. Without the descriptor the setting would be accepted on the Stream and
    // then dropped when the routes were folded into one consumer.
    record Case(String setting, UnaryOperator<Stream<byte[]>> apply) {}
    final var cases = List.of(
      new Case("withMetricsLog", s -> s.withMetricsLog(Duration.ofSeconds(1))),
      new Case("withShutdownHook", s -> s.withShutdownHook(true)),
      new Case("withThreadTerminationTimeout", s -> s.withThreadTerminationTimeout(Duration.ofSeconds(1))),
      new Case("withWaitForMessagesTimeout", s -> s.withWaitForMessagesTimeout(Duration.ofSeconds(1))),
      new Case("withCommitInterval", s -> s.withCommitInterval(Duration.ofSeconds(1))),
      new Case("withOffsetStore", s -> s.withOffsetStore(STORE))
    );
    for (final var c : cases) {
      final var multi = KPipe.multi(props()).bytes("topic-a", s ->
        c
          .apply()
          .apply(s)
          .toCustom(_ -> {})
      );
      final var ex = assertThrows(IllegalArgumentException.class, multi::start, c.setting());
      assertTrue(ex.getMessage().contains(c.setting()), () -> "should name " + c.setting() + ": " + ex.getMessage());
      assertTrue(ex.getMessage().contains("'topic-a'"), () -> "should name the route: " + ex.getMessage());
      assertTrue(
        ex.getMessage().contains("MultiBuilder." + c.setting()),
        () -> "should point at the mirror: " + ex.getMessage()
      );
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
