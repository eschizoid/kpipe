package io.github.eschizoid.kpipe;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.eschizoid.kpipe.consumer.KPipeConsumer;
import io.github.eschizoid.kpipe.consumer.OffsetManager;
import io.github.eschizoid.kpipe.consumer.OffsetState;
import io.github.eschizoid.kpipe.consumer.OffsetStatistics;
import io.github.eschizoid.kpipe.consumer.ProcessingMode;
import io.github.eschizoid.kpipe.registry.MessageFormat;
import io.github.eschizoid.kpipe.registry.MessageProcessorRegistry;
import io.github.eschizoid.kpipe.test.CapturingSink;
import java.time.Duration;
import java.util.Collection;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.BooleanSupplier;
import java.util.function.UnaryOperator;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

/// Docker-free end-to-end test of the raw-bytes passthrough path this example module demonstrates.
///
/// Two raw payloads (one binary, one longer than the app's 16-byte hex-preview window) are seeded
/// into a [MockConsumer] and driven through a real, started [KPipeConsumer] whose pipeline is
/// built from `MessageFormat.bytes()` — the same identity format `KPipe.bytes(topic, props)`
/// wires in `App`. The pipeline carries one operator mirroring the app's sink behavior (length +
/// truncated hex preview) so the test pins BOTH halves of the example: the bytes reach the sink
/// completely untouched (no SerDe, no copy mutation), and the preview rendering the app logs is
/// byte-for-byte what an operator observes mid-pipeline.
class AppIntegrationTest {

  private static final String TOPIC = "raw-bytes";
  private static final int PREVIEW_BYTES = 16;

  /// A short binary payload including non-UTF-8 bytes — raw passthrough must not care.
  private static final byte[] BINARY_PAYLOAD = { (byte) 0xDE, (byte) 0xAD, (byte) 0xBE, (byte) 0xEF };

  /// Twenty sequential bytes, longer than the preview window, to pin the `...` truncation.
  private static final byte[] LONG_PAYLOAD = new byte[20];

  static {
    for (var i = 0; i < LONG_PAYLOAD.length; i++) LONG_PAYLOAD[i] = (byte) i;
  }

  @Test
  void deliversRawBytesUntouchedEndToEnd() throws Exception {
    // Operator mirroring App's hexPreviewSink: observe length + preview, pass bytes through.
    final var previews = new CopyOnWriteArrayList<String>();
    final UnaryOperator<byte[]> hexPreviewOperator = payload -> {
      previews.add(payload.length + ":" + hexPreview(payload));
      return payload;
    };

    final var capturingSink = new CapturingSink<byte[]>();
    final var pipeline = new MessageProcessorRegistry()
      .pipeline(MessageFormat.bytes())
      .add(hexPreviewOperator)
      .toSink(capturingSink)
      .build();

    final var offsetManager = new MarkRecordingOffsetManager();
    final var mock = seededWith(BINARY_PAYLOAD, LONG_PAYLOAD);

    final var consumer = KPipeConsumer.builder()
      .withProperties(consumerProps())
      .withTopic(TOPIC)
      // SEQUENTIAL so capture order equals seed order and assertions are deterministic.
      .withProcessingMode(ProcessingMode.SEQUENTIAL)
      .withPipeline(pipeline)
      .withConsumer(() -> mock)
      .withOffsetManager(offsetManager)
      .withPollTimeout(Duration.ofMillis(5))
      .build();

    try {
      consumer.start();
      awaitCondition(
        () -> capturingSink.count() >= 2 && offsetManager.marked.containsAll(Set.of(0L, 1L)),
        10_000
      );

      final var delivered = capturingSink.captured();
      assertAll(
        () -> assertEquals(2, delivered.size(), "both seeded records must reach the sink"),
        () -> assertArrayEquals(BINARY_PAYLOAD, delivered.get(0), "binary payload must arrive untouched"),
        () -> assertArrayEquals(LONG_PAYLOAD, delivered.get(1), "long payload must arrive untouched"),
        // The exact preview strings the app would log — literal expectations, not recomputed.
        () -> assertEquals("4:de ad be ef", previews.get(0)),
        () -> assertEquals("20:00 01 02 03 04 05 06 07 08 09 0a 0b 0c 0d 0e 0f ...", previews.get(1))
      );
      assertTrue(
        offsetManager.marked.containsAll(Set.of(0L, 1L)),
        "both offsets must be marked processed (no re-fetch loop)"
      );
    } finally {
      consumer.close();
    }

    assertFalse(consumer.isRunning(), "consumer must reach terminal state after close()");
  }

  /// Replicates `App.hexPreview` (private there): first 16 bytes, space-delimited hex, `...`
  /// suffix when truncated. The expected values in the test are literal strings, so this helper
  /// only feeds the operator — it is asserted against, never asserted with.
  private static String hexPreview(final byte[] payload) {
    final var limit = Math.min(payload.length, PREVIEW_BYTES);
    final var hex = HexFormat.ofDelimiter(" ").formatHex(payload, 0, limit);
    return payload.length > limit ? hex + " ..." : hex;
  }

  private static Properties consumerProps() {
    final var props = new Properties();
    props.put("bootstrap.servers", "localhost:9092");
    props.put("group.id", "bytes-test-group");
    props.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
    props.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
    props.put("enable.auto.commit", "false");
    return props;
  }

  /// Seeds the given payloads at offsets 0..n-1. `subscribe` is stubbed to a no-op so the manual
  /// `assign` survives (the consumer subscribes by topic; the mock only honors the assign).
  private static MockConsumer<byte[], byte[]> seededWith(final byte[]... values) {
    final var mock = new MockConsumer<byte[], byte[]>("earliest") {
      @Override
      public synchronized void subscribe(final Collection<String> topics) {}

      @Override
      public synchronized void subscribe(final Collection<String> topics, final ConsumerRebalanceListener cb) {}
    };
    final var tp = new TopicPartition(TOPIC, 0);
    mock.assign(List.of(tp));
    mock.updateBeginningOffsets(Map.of(tp, 0L));
    for (var offset = 0; offset < values.length; offset++) {
      mock.addRecord(new ConsumerRecord<>(TOPIC, 0, offset, ("key-" + offset).getBytes(UTF_8), values[offset]));
    }
    mock.updateEndOffsets(Map.of(tp, (long) values.length));
    return mock;
  }

  private static void awaitCondition(final BooleanSupplier cond, final long timeoutMs) throws InterruptedException {
    final var deadline = System.currentTimeMillis() + timeoutMs;
    while (!cond.getAsBoolean()) {
      if (System.currentTimeMillis() >= deadline) throw new AssertionError(
        "timed out after " + timeoutMs + "ms waiting for the seeded records to deliver and mark"
      );
      Thread.sleep(10);
    }
  }

  /// Minimal offset manager that records which offsets were marked processed, so the test can
  /// assert the commit frontier advances past every seeded record.
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
