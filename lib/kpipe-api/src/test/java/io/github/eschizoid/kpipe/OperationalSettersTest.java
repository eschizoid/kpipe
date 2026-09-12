package io.github.eschizoid.kpipe;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.eschizoid.kpipe.metrics.KPipeMetricsReporter;
import java.time.Duration;
import java.util.List;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.junit.jupiter.api.Test;

/// Pins the five operational settings that moved from the explicit builder onto the fluent facade.
///
/// These configure how a consumer behaves in a deployment rather than how records are processed,
/// which is why they belong on the 80% path: reaching one used to mean abandoning `KPipe.json(...)`
/// and rebuilding through `KPipeConsumerBuilder` to obtain something orthogonal to the pipeline.
///
/// **Scope.** Each test asserts the setter reaches the immutable [ConsumerConfig] and that the
/// per-route guard knows about it. The single `applyTo` delegation to `KPipeConsumerBuilder` is not
/// executed here: the builder is final with package-private fields in another module's package, so
/// there is no read-back seam, and this repo does not use reflection in tests. That line is covered
/// by inspection and by the builder's own tests.
class OperationalSettersTest {

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
    final KPipeMetricsReporter reporter = () -> {};
    assertEquals(List.of(reporter), configOf(stream().withMetricsReporters(List.of(reporter))).metricsReporters());
    assertEquals(
      Duration.ofSeconds(30),
      configOf(stream().withMetricsInterval(Duration.ofSeconds(30))).metricsInterval()
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
  }

  @Test
  void unsetLeavesNullSoTheBuilderKeepsItsOwnDefault() {
    // Null is the facade's "unset" encoding — applyTo skips it, so the builder's default stands.
    // A setter defaulting to a concrete value here would silently override that default for
    // everyone, including callers who never touched the setting.
    final var c = configOf(stream());
    assertNull(c.metricsReporters());
    assertNull(c.metricsInterval());
    assertNull(c.shutdownHook());
    assertNull(c.threadTerminationTimeout());
    assertNull(c.waitForMessagesTimeout());
  }

  @Test
  void settersReturnNewStreamsAndLeaveTheOriginalAlone() {
    final var base = stream();
    final var derived = base.withShutdownHook(true).withMetricsInterval(Duration.ofSeconds(5));
    assertNull(configOf(base).shutdownHook(), "the original stream must not observe a later setter");
    assertNull(configOf(base).metricsInterval());
    assertEquals(Boolean.TRUE, configOf(derived).shutdownHook());
  }

  @Test
  void reporterCollectionIsCopiedSoLaterCallerMutationCannotLeakIn() {
    final KPipeMetricsReporter reporter = () -> {};
    final var mutable = new java.util.ArrayList<KPipeMetricsReporter>();
    mutable.add(reporter);
    final var s = stream().withMetricsReporters(mutable);
    mutable.clear();
    assertEquals(1, configOf(s).metricsReporters().size(), "the stream must hold its own copy");
  }

  @Test
  void nullArgumentsAreRejectedAtTheCallSite() {
    assertThrows(NullPointerException.class, () -> stream().withMetricsReporters(null));
    assertThrows(NullPointerException.class, () -> stream().withMetricsInterval(null));
    assertThrows(NullPointerException.class, () -> stream().withThreadTerminationTimeout(null));
    assertThrows(NullPointerException.class, () -> stream().withWaitForMessagesTimeout(null));
  }

  @Test
  void multiBuilderRejectsThemPerRouteAndNamesItsOwnMirror() {
    // Registering each in CONSUMER_WIDE_SETTINGS is what makes a route-level call an error rather
    // than a silent no-op. Without the descriptor the setting would be accepted on the Stream and
    // then dropped when the routes were folded into one consumer.
    record Case(String setting, java.util.function.UnaryOperator<Stream<byte[]>> apply) {}
    final var cases = List.of(
      new Case("withMetricsInterval", s -> s.withMetricsInterval(Duration.ofSeconds(1))),
      new Case("withShutdownHook", s -> s.withShutdownHook(true)),
      new Case("withThreadTerminationTimeout", s -> s.withThreadTerminationTimeout(Duration.ofSeconds(1))),
      new Case("withWaitForMessagesTimeout", s -> s.withWaitForMessagesTimeout(Duration.ofSeconds(1)))
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
    final KPipeMetricsReporter reporter = () -> {};
    KPipe.multi(props())
      .withMetricsReporters(List.of(reporter))
      .withMetricsInterval(Duration.ofSeconds(30))
      .withShutdownHook(true)
      .withThreadTerminationTimeout(Duration.ofSeconds(7))
      .withWaitForMessagesTimeout(Duration.ofSeconds(9))
      .bytes("topic-a", s -> s.toCustom(_ -> {}));
  }
}
