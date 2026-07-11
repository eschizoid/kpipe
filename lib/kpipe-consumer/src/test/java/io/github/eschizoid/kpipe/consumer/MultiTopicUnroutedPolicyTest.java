package io.github.eschizoid.kpipe.consumer;

import static java.nio.charset.StandardCharsets.UTF_8;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.eschizoid.kpipe.registry.MessagePipeline;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

/// Pins the multi-topic unrouted-topic policy and the config-error rejection that the
/// heterogeneous dispatch path (`withPipelines`) promises.
///
/// **Unrouted-topic policy.** A record for a topic that has no registered pipeline (a rebalance
/// race or a config error) must be:
///
///   * dropped (no DLQ — there is no per-topic DLQ to route it to),
///   * logged at WARNING, and
///   * marked processed — so its offset advances and the same record is NOT re-fetched forever.
///
/// The consumer thread must NOT crash: a throw here would take down processing for every other
/// (correctly routed) topic on the same consumer. This test drives a real [KPipeConsumer] through
/// [MockConsumer] (no Docker) with one routed topic and one unrouted topic seeded side by side,
/// and asserts the routed topic keeps flowing while the unrouted record is dropped + marked.
///
/// **Config-error rejection.** Mixing `withTopic`/`withTopics` with `withPipelines` is rejected at
/// `build()` — a silent override would mask the user's mistake about which topic set wins.
class MultiTopicUnroutedPolicyTest {

  private static final String ROUTED_TOPIC = "routed-topic";
  private static final String UNROUTED_TOPIC = "unrouted-topic";

  private static Properties consumerProps() {
    final var p = new Properties();
    p.put("bootstrap.servers", "localhost:9092");
    p.put("group.id", "test-group");
    p.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    p.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
    return p;
  }

  /// MockConsumer whose `subscribe(...)` is a no-op — the consumer's `subscribe(topic, listener)`
  /// call must not clear the partitions the test pre-assigns. Records are delivered from whatever
  /// the test `assign`s + `addRecord`s, independent of the subscribed topic set, which is exactly
  /// the rebalance-race shape: a record lands for a topic the routing map never registered.
  private static MockConsumer<byte[], byte[]> newMockConsumer() {
    return new MockConsumer<>("earliest") {
      @Override
      public synchronized void subscribe(final Collection<String> topics) {}

      @Override
      public synchronized void subscribe(final Collection<String> topics, final ConsumerRebalanceListener cb) {}
    };
  }

  /// Counts every `markOffsetProcessed` call per `topic-partition-offset`, delegating everything
  /// else to a real [KafkaOffsetManager] so commit semantics stay genuine. The recorded marks are
  /// the ground truth for "the unrouted record's offset advanced — it won't be re-fetched forever."
  private static final class RecordingOffsetManager implements OffsetManager {

    private final OffsetManager delegate;
    private final Map<String, AtomicInteger> markCounts;

    private RecordingOffsetManager(final OffsetManager delegate, final Map<String, AtomicInteger> markCounts) {
      this.delegate = delegate;
      this.markCounts = markCounts;
    }

    private static String key(final ConsumerRecord<?, byte[]> record) {
      return record.topic() + "-" + record.partition() + "-" + record.offset();
    }

    @Override
    public OffsetManager start() {
      delegate.start();
      return this;
    }

    @Override
    public OffsetManager stop() {
      delegate.stop();
      return this;
    }

    @Override
    public void trackOffset(final ConsumerRecord<byte[], byte[]> record) {
      delegate.trackOffset(record);
    }

    @Override
    public void markOffsetProcessed(final ConsumerRecord<byte[], byte[]> record) {
      markCounts.computeIfAbsent(key(record), k -> new AtomicInteger()).incrementAndGet();
      delegate.markOffsetProcessed(record);
    }

    @Override
    public void notifyCommitComplete(final String commitId, final boolean success) {
      delegate.notifyCommitComplete(commitId, success);
    }

    @Override
    public ConsumerRebalanceListener createRebalanceListener() {
      return delegate.createRebalanceListener();
    }

    @Override
    public OffsetState getState() {
      return delegate.getState();
    }

    @Override
    public boolean isRunning() {
      return delegate.isRunning();
    }

    @Override
    public Map<String, Object> getStatistics() {
      return delegate.getStatistics();
    }

    @Override
    public void close() {
      delegate.close();
    }
  }

  private static void awaitCondition(final BooleanSupplier cond, final long timeoutMs) throws InterruptedException {
    final long deadline = System.currentTimeMillis() + timeoutMs;
    while (!cond.getAsBoolean()) {
      if (System.currentTimeMillis() >= deadline) throw new AssertionError(
        "awaitCondition timed out after " + timeoutMs + "ms"
      );
      Thread.sleep(20);
    }
  }

  /// A record arrives for a topic with no registered pipeline (rebalance race / config error):
  /// it is dropped, logged at WARNING, and its offset IS marked processed exactly once (so the
  /// record is not re-fetched forever); meanwhile the routed topic keeps being processed and the
  /// consumer thread never crashes.
  @Test
  void unroutedRecordIsDroppedLoggedAndMarkedWithoutCrashingTheConsumer() throws Exception {
    // Seed MockConsumer with one record on a routed topic and one on an unrouted topic. Both
    // partitions are assigned so the poll loop delivers both, even though only the routed topic
    // has a pipeline registered via withPipelines.
    final var mock = newMockConsumer();
    final var routedTp = new TopicPartition(ROUTED_TOPIC, 0);
    final var unroutedTp = new TopicPartition(UNROUTED_TOPIC, 0);

    mock.assign(List.of(routedTp, unroutedTp));
    final var beginning = new HashMap<TopicPartition, Long>();
    beginning.put(routedTp, 0L);
    beginning.put(unroutedTp, 0L);
    mock.updateBeginningOffsets(beginning);

    mock.addRecord(new ConsumerRecord<>(ROUTED_TOPIC, 0, 0L, "k-routed".getBytes(UTF_8), "routed-payload".getBytes()));
    mock.addRecord(new ConsumerRecord<>(UNROUTED_TOPIC, 0, 0L, "k-unrouted".getBytes(UTF_8), "unrouted-payload".getBytes()));

    final var end = new HashMap<TopicPartition, Long>();
    end.put(routedTp, 1L);
    end.put(unroutedTp, 1L);
    mock.updateEndOffsets(end);

    // Capture WARNING logs emitted by KPipeConsumer (System.Logger → JUL by default).
    final var julLogger = Logger.getLogger(KPipeConsumer.class.getName());
    final var captured = new CopyOnWriteArrayList<LogRecord>();
    final var handler = new Handler() {
      @Override
      public void publish(final LogRecord record) {
        if (record.getLevel().intValue() >= Level.WARNING.intValue()) captured.add(record);
      }

      @Override
      public void flush() {}

      @Override
      public void close() {}
    };
    final var originalLevel = julLogger.getLevel();
    final var originalUseParent = julLogger.getUseParentHandlers();
    julLogger.addHandler(handler);
    julLogger.setLevel(Level.ALL);
    julLogger.setUseParentHandlers(false);

    final var markCounts = new ConcurrentHashMap<String, AtomicInteger>();
    final var routedProcessed = new AtomicInteger(0);
    final var sharedQueue = new ConcurrentLinkedQueue<ConsumerCommand>();

    final MessagePipeline<byte[]> routedPipeline = TestPipelines.sideEffect(v -> {
      routedProcessed.incrementAndGet();
      return v;
    });

    final var consumer = KPipeConsumer.builder()
      .withProperties(consumerProps())
      .withPipelines(Map.of(ROUTED_TOPIC, routedPipeline))
      .withConsumer(() -> mock)
      .withCommandQueue(sharedQueue)
      .withOffsetManagerProvider(c -> wrapWithRecorder(c, sharedQueue, markCounts))
      .build();

    consumer.start();
    try {
      // Both records must drain: routed → processed, unrouted → marked-and-dropped.
      awaitCondition(() -> routedProcessed.get() >= 1 && markCounts.containsKey(UNROUTED_TOPIC + "-0-0"), 10_000);

      // The consumer thread survived the unrouted record (no crash) and is still running.
      assertTrue(consumer.isRunning(), "consumer must remain running after dropping an unrouted record");

      // Unrouted record was marked processed exactly once → its offset advances, no re-fetch loop.
      final var unroutedMarks = markCounts.get(UNROUTED_TOPIC + "-0-0");
      assertEquals(
        1,
        unroutedMarks == null ? 0 : unroutedMarks.get(),
        "unrouted record must be marked processed exactly once (offset advances, no infinite re-fetch)"
      );

      // The routed topic kept flowing — the unrouted drop did not poison the shared consumer.
      assertEquals(1, routedProcessed.get(), "routed topic must continue processing alongside an unrouted topic");

      // A WARNING naming the unrouted topic was logged; the dropped record's offset is mentioned.
      final var dropWarn = captured
        .stream()
        .filter(r -> r.getMessage() != null && r.getMessage().contains("No pipeline registered"))
        .filter(r -> {
          final var params = r.getParameters();
          return params != null && params.length >= 1 && UNROUTED_TOPIC.equals(params[0]);
        })
        .findFirst()
        .orElse(null);
      assertTrue(dropWarn != null, "expected a WARNING naming the unrouted topic; captured=" + captured.size());
    } finally {
      julLogger.removeHandler(handler);
      julLogger.setLevel(originalLevel);
      julLogger.setUseParentHandlers(originalUseParent);
      consumer.close();
    }

    // Each record is delivered by MockConsumer once; the genuine no-re-fetch guarantee is the
    // "marked processed" assertion above. Belt-and-suspenders: the unrouted payload never reached
    // the routed sink (it was dropped, not misrouted).
    assertEquals(1, routedProcessed.get(), "no double-processing after close");
  }

  private static OffsetManager wrapWithRecorder(
    final Consumer<byte[], byte[]> consumer,
    final Queue<ConsumerCommand> queue,
    final Map<String, AtomicInteger> markCounts
  ) {
    final var real = KafkaOffsetManager.builder(consumer).withCommandQueue(queue).build();
    return new RecordingOffsetManager(real, markCounts);
  }

  /// Mixing `withTopic` with `withPipelines` is a config error: the topic set is derived from the
  /// pipeline-map keys, so an explicit topic call is ambiguous and rejected at `build()`.
  @Test
  void mixingWithTopicAndWithPipelinesIsRejected() {
    final var builder = KPipeConsumer.builder()
      .withProperties(consumerProps())
      .withTopic(ROUTED_TOPIC)
      .withPipelines(Map.of(ROUTED_TOPIC, TestPipelines.identity()));

    final var ex = assertThrows(IllegalArgumentException.class, builder::build);
    assertTrue(
      ex.getMessage().contains("withPipelines"),
      "rejection message must point the user at the withTopic/withPipelines conflict; got: " + ex.getMessage()
    );
  }

  /// Same rejection via the multi-arg `withTopics` setter — both topic-setting entry points must
  /// conflict with `withPipelines`, not just the single-topic form.
  @Test
  void mixingWithTopicsAndWithPipelinesIsRejected() {
    final var builder = KPipeConsumer.builder()
      .withProperties(consumerProps())
      .withTopics(ROUTED_TOPIC, "another-topic")
      .withPipelines(Map.of(ROUTED_TOPIC, TestPipelines.identity()));

    assertThrows(IllegalArgumentException.class, builder::build);
  }
}
