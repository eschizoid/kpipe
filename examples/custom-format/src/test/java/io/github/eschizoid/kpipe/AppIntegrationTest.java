package io.github.eschizoid.kpipe;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.eschizoid.kpipe.App.CsvOrder;
import io.github.eschizoid.kpipe.App.CsvOrderFormat;
import io.github.eschizoid.kpipe.consumer.KPipeConsumer;
import io.github.eschizoid.kpipe.consumer.KPipeConsumer.ProcessingError;
import io.github.eschizoid.kpipe.consumer.OffsetManager;
import io.github.eschizoid.kpipe.consumer.OffsetState;
import io.github.eschizoid.kpipe.consumer.OffsetStatistics;
import io.github.eschizoid.kpipe.consumer.ProcessingMode;
import io.github.eschizoid.kpipe.registry.MessageProcessorRegistry;
import io.github.eschizoid.kpipe.test.CapturingSink;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.BooleanSupplier;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

/// Docker-free end-to-end test of the user-supplied [CsvOrderFormat] this example module ships —
/// the `KPipe.custom(topic, props, format)` path where the codec is the user's own code.
///
/// CSV-line payloads are seeded into a [MockConsumer] and driven through a real, started
/// [KPipeConsumer] whose pipeline mirrors `App` operator-for-operator: the app's OWN
/// [CsvOrderFormat] decodes, a quantity filter drops zero-quantity orders, and an uppercase-SKU
/// transform reshapes what survives. Four seeded lines pin the four behaviors:
///
/// - a whitespace-padded line with a trailing newline decodes and is trimmed field-by-field
/// - a zero-quantity order is FILTERED (offset marked, sink never sees it, no error)
/// - a malformed line (wrong field count) FAILS — the codec throws, the error handler observes
///   the failure, and with no DLQ configured the offset is still marked (log-and-advance)
/// - a plain valid line decodes and is transformed
class AppIntegrationTest {

  private static final String TOPIC = "csv-orders";

  @Test
  void decodesFiltersTransformsAndSurfacesMalformedLinesEndToEnd() throws Exception {
    final var errors = new CopyOnWriteArrayList<ProcessingError>();
    final var capturingSink = new CapturingSink<CsvOrder>();
    final var pipeline = new MessageProcessorRegistry()
      .pipeline(new CsvOrderFormat())
      // Mirrors App's .filter(order -> order.quantity() > 0): null return = intentional filter.
      .add(order -> order.quantity() > 0 ? order : null)
      // Mirrors App's .pipe(...): uppercase the SKU.
      .add(order -> new CsvOrder(order.id(), order.sku().toUpperCase(), order.quantity()))
      .toSink(capturingSink)
      .build();

    final var offsetManager = new MarkRecordingOffsetManager();
    final var mock = seededWith(
      "o-1, widget ,3\n", // offset 0: padded + trailing newline, decodes to ("o-1","widget",3)
      "o-2,gadget,0", // offset 1: quantity 0 -> filtered, never reaches the sink
      "definitely-not-csv", // offset 2: one field -> codec throws -> error path
      "o-3,doohickey,7" // offset 3: plain valid line
    );

    final var consumer = KPipeConsumer.builder()
      .withProperties(consumerProps())
      .withTopic(TOPIC)
      // SEQUENTIAL so capture order equals seed order and assertions are deterministic.
      .withProcessingMode(ProcessingMode.SEQUENTIAL)
      .withPipeline(pipeline)
      .withConsumer(() -> mock)
      .withOffsetManager(offsetManager)
      .withErrorHandler(errors::add)
      .withPollTimeout(Duration.ofMillis(5))
      .build();

    try {
      consumer.start();
      awaitCondition(
        () ->
          capturingSink.count() >= 2 &&
          errors.size() >= 1 &&
          offsetManager.marked.containsAll(Set.of(0L, 1L, 2L, 3L)),
        10_000
      );

      // The two surviving orders, decoded by the app's codec and transformed by the app's
      // operators — exact values, in seed order.
      assertEquals(
        List.of(new CsvOrder("o-1", "WIDGET", 3), new CsvOrder("o-3", "DOOHICKEY", 7)),
        capturingSink.captured(),
        "only the two positive-quantity orders reach the sink, trimmed and uppercased"
      );

      // The malformed line surfaced as a failure, not a silent filter: the codec's
      // IllegalArgumentException is the root cause behind the pipeline's diagnostic wrapper.
      assertEquals(1, errors.size(), "exactly one record must fail: the malformed line");
      final var failure = errors.getFirst();
      assertAll(
        () -> assertEquals(2L, failure.record().offset(), "the failure is the malformed record at offset 2"),
        () -> assertInstanceOf(
          IllegalArgumentException.class,
          failure.exception().getCause(),
          "the codec's IllegalArgumentException must be preserved as the cause"
        )
      );

      // All four offsets marked: passed (0, 3), filtered (1), and failed-without-DLQ (2) all
      // advance the commit frontier — nothing loops on re-fetch.
      assertTrue(offsetManager.marked.containsAll(Set.of(0L, 1L, 2L, 3L)), "every offset must be marked processed");
    } finally {
      consumer.close();
    }

    assertFalse(consumer.isRunning(), "consumer must reach terminal state after close()");
  }

  /// Pins the codec's malformed-input contract directly: it must THROW on bad bytes, never
  /// return null (null deserialization would be misread as intentional filtering downstream).
  @Test
  void codecThrowsOnMalformedInputAndRoundTripsValidOrders() {
    final var format = new CsvOrderFormat();

    // Round-trip: serialize then deserialize yields the identical record.
    final var order = new CsvOrder("o-9", "sprocket", 12);
    assertEquals(order, format.deserialize(format.serialize(order)));
    assertEquals("o-9,sprocket,12", new String(format.serialize(order), UTF_8));

    assertAll(
      () -> assertThrows(IllegalArgumentException.class, () -> format.deserialize(new byte[0]), "empty payload"),
      () -> assertThrows(
        IllegalArgumentException.class,
        () -> format.deserialize("only,two".getBytes(UTF_8)),
        "wrong field count"
      ),
      () -> assertThrows(
        IllegalArgumentException.class,
        () -> format.deserialize("o-1,widget,many".getBytes(UTF_8)),
        "non-integer quantity"
      )
    );
  }

  private static Properties consumerProps() {
    final var props = new Properties();
    props.put("bootstrap.servers", "localhost:9092");
    props.put("group.id", "custom-format-test-group");
    props.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
    props.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
    props.put("enable.auto.commit", "false");
    return props;
  }

  /// Seeds the given CSV lines (as UTF-8 bytes) at offsets 0..n-1. `subscribe` is stubbed to a
  /// no-op so the manual `assign` survives (the consumer subscribes by topic; the mock only
  /// honors the assign).
  private static MockConsumer<byte[], byte[]> seededWith(final String... csvLines) {
    final var mock = new MockConsumer<byte[], byte[]>("earliest") {
      @Override
      public synchronized void subscribe(final Collection<String> topics) {}

      @Override
      public synchronized void subscribe(final Collection<String> topics, final ConsumerRebalanceListener cb) {}
    };
    final var tp = new TopicPartition(TOPIC, 0);
    mock.assign(List.of(tp));
    mock.updateBeginningOffsets(Map.of(tp, 0L));
    for (var offset = 0; offset < csvLines.length; offset++) {
      mock.addRecord(
        new ConsumerRecord<>(TOPIC, 0, offset, ("key-" + offset).getBytes(UTF_8), csvLines[offset].getBytes(UTF_8))
      );
    }
    mock.updateEndOffsets(Map.of(tp, (long) csvLines.length));
    return mock;
  }

  private static void awaitCondition(final BooleanSupplier cond, final long timeoutMs) throws InterruptedException {
    final var deadline = System.currentTimeMillis() + timeoutMs;
    while (!cond.getAsBoolean()) {
      if (System.currentTimeMillis() >= deadline) throw new AssertionError(
        "timed out after " + timeoutMs + "ms waiting for the seeded records to decode, filter, fail, and mark"
      );
      Thread.sleep(10);
    }
  }

  /// Minimal offset manager that records which offsets were marked processed, so the test can
  /// assert the commit frontier advances past every seeded record — including the filtered and
  /// the failed one.
  private static final class MarkRecordingOffsetManager implements OffsetManager {

    final Set<Long> marked = ConcurrentHashMap.newKeySet();

    @Override
    public OffsetManager start() {
      return this;
    }

    @Override
    public OffsetManager stop() {
      return this;
    }

    @Override
    public void trackOffset(final ConsumerRecord<byte[], byte[]> record) {}

    @Override
    public void markOffsetProcessed(final ConsumerRecord<byte[], byte[]> record) {
      marked.add(record.offset());
    }

    @Override
    public void notifyCommitComplete(final String commitId, final boolean success) {}

    @Override
    public OffsetState getState() {
      return OffsetState.CREATED;
    }

    @Override
    public boolean isRunning() {
      return true;
    }

    @Override
    public OffsetStatistics getStatistics() {
      return OffsetStatistics.empty();
    }

    @Override
    public void close() {}
  }
}
