package io.github.eschizoid.kpipe.consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.github.eschizoid.kpipe.sink.BatchPolicy;
import io.github.eschizoid.kpipe.sink.BatchSink;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/// Repeated lifecycle stress: build a fresh consumer, start it, then tear it down (clean close or
/// self-termination crash) many times in a row, asserting nothing accumulates across iterations.
///
/// A `KPipeConsumer` is single-use — `start()` does `compareAndSet(CREATED, RUNNING)`, so it cannot
/// be restarted after close. "Repeated cycles" therefore means a brand-new consumer per iteration;
/// the leak hazard is that each cycle's resources (the Kafka consumer, the offset manager, the
/// dispatcher's virtual-thread executor, the scheduler, batch wrappers, metrics-reporter threads)
/// fail to release, so live threads / open consumers pile up over time.
///
/// The probes here are deliberately observable rather than reflective:
///
///   * Every iteration uses its own [MockConsumer]; `closed()` proves the Kafka consumer was
///     released.
///   * Every iteration uses its own recording [OffsetManager]; its `close()` count proves the
///     teardown ran exactly once.
///   * The consumer reaches `CLOSED` every time (`isRunning()` false, `awaitShutdown` true).
///   * A live-thread census taken before vs. after the whole loop bounds thread growth: the named
///     kpipe / kafka-consumer threads must not accumulate one-per-iteration. Virtual threads are
///     unmounted and not individually introspectable, but the scheduler / metrics-reporter are
///     platform-backed (or named-virtual) and their carrier/daemon threads are what would leak if
///     `releaseConstructedResources` skipped a shutdown.
class LifecycleLeakCycleTest {

  private static final String TOPIC = "lifecycle-topic";
  private static final int CYCLES = 25;

  private Properties props() {
    final var p = new Properties();
    p.put("bootstrap.servers", "localhost:9092");
    p.put("group.id", "lifecycle-group");
    p.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    p.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
    p.put("enable.auto.commit", "false");
    return p;
  }

  /// Counts its own `close()` invocations and offset marks so a double-release or a skipped release
  /// is observable. One instance per cycle.
  private static final class CountingOffsetManager implements OffsetManager<String> {

    final AtomicInteger closeCount = new AtomicInteger(0);
    final AtomicInteger marks = new AtomicInteger(0);

    @Override
    public OffsetManager<String> start() {
      return this;
    }

    @Override
    public OffsetManager<String> stop() {
      return this;
    }

    @Override
    public void trackOffset(final ConsumerRecord<String, byte[]> record) {}

    @Override
    public void markOffsetProcessed(final ConsumerRecord<String, byte[]> record) {
      marks.incrementAndGet();
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
    public Map<String, Object> getStatistics() {
      return Map.of();
    }

    @Override
    public void close() {
      closeCount.incrementAndGet();
    }
  }

  private static MockConsumer<String, byte[]> nonSubscribingMock() {
    return new MockConsumer<>("earliest") {
      @Override
      public synchronized void subscribe(final Collection<String> topics) {}

      @Override
      public synchronized void subscribe(final Collection<String> topics, final ConsumerRebalanceListener cb) {}
    };
  }

  private static MockConsumer<String, byte[]> seeded(final int recordCount) {
    final var mock = nonSubscribingMock();
    final var tp = new TopicPartition(TOPIC, 0);
    mock.assign(List.of(tp));
    mock.updateBeginningOffsets(Map.of(tp, 0L));
    for (int i = 0; i < recordCount; i++) {
      mock.addRecord(new ConsumerRecord<>(TOPIC, 0, i, "k" + i, ("v" + i).getBytes()));
    }
    mock.updateEndOffsets(Map.of(tp, (long) recordCount));
    return mock;
  }

  /// Snapshot of live thread names that the consumer might spawn. Virtual worker threads mount on
  /// shared carriers and aren't reliably countable; the names below are the ones a leaked teardown
  /// would strand: the scheduler carrier, the metrics reporter, the named consumer thread.
  private static Set<String> kpipeThreadNames() {
    return Thread.getAllStackTraces()
      .keySet()
      .stream()
      .map(Thread::getName)
      .filter(n -> n.startsWith("kpipe-") || n.startsWith("kafka-consumer-"))
      .collect(Collectors.toSet());
  }

  @ParameterizedTest
  @EnumSource(ProcessingMode.class)
  void repeatedBuildStartCloseReleasesEverythingEachCycle(final ProcessingMode mode) throws Exception {
    // Clean lifecycle: build → start → process a few records → close, repeated. Each cycle must
    // close its own Kafka consumer and offset manager exactly once and reach CLOSED. A leak would
    // show up as a mock left open, a manager closed != once, or a consumer stuck non-CLOSED.
    final var threadsBefore = kpipeThreadNames();

    for (int cycle = 0; cycle < CYCLES; cycle++) {
      final var mock = seeded(5);
      final var manager = new CountingOffsetManager();
      final var processed = new AtomicInteger(0);

      final var consumer = KPipeConsumer.<String>builder()
        .withProperties(props())
        .withTopic(TOPIC)
        .withProcessingMode(mode)
        .withPipeline(
          TestPipelines.sideEffect(v -> {
            processed.incrementAndGet();
            return v;
          })
        )
        .withConsumer(() -> mock)
        .withOffsetManager(manager)
        .withPollTimeout(Duration.ofMillis(5))
        .withThreadTerminationTimeout(Duration.ofSeconds(2))
        .build();

      consumer.start();
      // Don't require all 5 — the point is the consumer is genuinely running before we close.
      final var deadline = System.nanoTime() + Duration.ofSeconds(5).toNanos();
      while (processed.get() == 0 && System.nanoTime() < deadline) Thread.sleep(2);
      consumer.close();

      assertTrue(consumer.awaitShutdown(Duration.ofSeconds(5)), () -> "cycle must reach CLOSED for mode " + mode);
      assertFalse(consumer.isRunning(), () -> "consumer must not be running after close for mode " + mode);
      assertTrue(mock.closed(), () -> "cycle must close its Kafka consumer for mode " + mode);
      assertEquals(
        1,
        manager.closeCount.get(),
        () -> "offset manager must be closed exactly once per clean cycle for mode " + mode
      );
    }

    assertThreadsDidNotAccumulate(threadsBefore, mode);
  }

  @ParameterizedTest
  @EnumSource(ProcessingMode.class)
  void repeatedBuildStartCrashReleasesEverythingEachCycle(final ProcessingMode mode) throws Exception {
    // Crash lifecycle: the consumer thread self-terminates because poll() throws an Error that
    // escapes the loop's catch(Exception). The thread's own finally must still run
    // releaseConstructedResources — closing the Kafka consumer + offset manager — without any
    // external close(). Repeated to prove the crash path doesn't leak per iteration.
    final var threadsBefore = kpipeThreadNames();

    for (int cycle = 0; cycle < CYCLES; cycle++) {
      final var manager = new CountingOffsetManager();
      final var poisoned = new AtomicBoolean(false);
      final var pollEntered = new CountDownLatch(1);
      final var tp = new TopicPartition(TOPIC, 0);

      final var mock = new MockConsumer<String, byte[]>("earliest") {
        @Override
        public synchronized void subscribe(final Collection<String> topics) {}

        @Override
        public synchronized void subscribe(final Collection<String> topics, final ConsumerRebalanceListener cb) {}

        @Override
        public synchronized ConsumerRecords<String, byte[]> poll(final Duration timeout) {
          pollEntered.countDown();
          if (poisoned.get()) throw new Error("simulated consumer-thread crash on cycle");
          return ConsumerRecords.empty();
        }
      };
      mock.assign(List.of(tp));
      mock.updateBeginningOffsets(Map.of(tp, 0L));

      final var consumer = KPipeConsumer.<String>builder()
        .withProperties(props())
        .withTopic(TOPIC)
        .withProcessingMode(mode)
        .withPipeline(TestPipelines.identity())
        .withConsumer(() -> mock)
        .withOffsetManager(manager)
        .withPollTimeout(Duration.ofMillis(5))
        .withThreadTerminationTimeout(Duration.ofSeconds(2))
        .build();

      consumer.start();
      assertTrue(pollEntered.await(2, TimeUnit.SECONDS), "consumer thread must reach poll() before we poison it");
      poisoned.set(true);

      // The crash drives teardown from the thread's own finally. Wait for the manager to be
      // closed — that's the last-but-one step of releaseConstructedResources.
      final var deadline = System.nanoTime() + Duration.ofSeconds(5).toNanos();
      while (manager.closeCount.get() == 0 && System.nanoTime() < deadline) Thread.sleep(5);

      assertEquals(
        1,
        manager.closeCount.get(),
        () -> "self-terminating crash must release the offset manager exactly once for mode " + mode
      );
      assertTrue(
        consumer.awaitShutdown(Duration.ofSeconds(5)),
        () -> "crashed consumer must reach CLOSED for mode " + mode
      );
      assertFalse(consumer.isRunning(), () -> "crashed consumer must not be running for mode " + mode);

      // A late external close() after a self-terminating crash must be a clean no-op — not a
      // second release (which would double-close the manager).
      consumer.close();
      assertEquals(
        1,
        manager.closeCount.get(),
        () -> "external close() after a crash must not re-release resources for mode " + mode
      );
    }

    assertThreadsDidNotAccumulate(threadsBefore, mode);
  }

  @Test
  void repeatedCycleWithFullFeatureStackDoesNotLeak() throws Exception {
    // Heaviest per-cycle resource footprint: PARALLEL dispatcher executor + scheduler (needed by
    // both batch wrappers and the circuit breaker) + a batch wrapper + a DLQ producer-less retry
    // path. If any of these aren't torn down per cycle, the scheduler carrier or batch wrapper
    // age-tick task would strand threads that the census catches.
    final var threadsBefore = kpipeThreadNames();

    for (int cycle = 0; cycle < CYCLES; cycle++) {
      final var mock = seeded(4);
      final var manager = new CountingOffsetManager();
      final var flushed = new AtomicInteger(0);

      final var consumer = KPipeConsumer.<String>builder()
        .withProperties(props())
        .withBatchPipeline(
          TOPIC,
          TestPipelines.sideEffect(v -> v),
          BatchSink.ofVoid(batch -> flushed.addAndGet(batch.size())),
          BatchPolicy.ofSize(2)
        )
        .withProcessingMode(ProcessingMode.PARALLEL)
        .withCircuitBreaker(new CircuitBreakerController(0.5, 10, Duration.ofSeconds(30)))
        .withRetry(1, Duration.ofMillis(1))
        .withConsumer(() -> mock)
        .withOffsetManager(manager)
        .withPollTimeout(Duration.ofMillis(5))
        .withThreadTerminationTimeout(Duration.ofSeconds(2))
        .build();

      consumer.start();
      final var deadline = System.nanoTime() + Duration.ofSeconds(5).toNanos();
      while (flushed.get() == 0 && System.nanoTime() < deadline) Thread.sleep(2);
      consumer.close();

      assertTrue(consumer.awaitShutdown(Duration.ofSeconds(5)), "full-stack cycle must reach CLOSED");
      assertTrue(mock.closed(), "full-stack cycle must close its Kafka consumer");
      assertEquals(1, manager.closeCount.get(), "full-stack cycle must close the offset manager exactly once");
    }

    assertThreadsDidNotAccumulate(threadsBefore, ProcessingMode.PARALLEL);
  }

  /// Bounds thread growth across the whole loop. Newly-stranded kpipe / kafka-consumer threads must
  /// not number on the order of `CYCLES`. A small slack absorbs late-joining carrier threads and
  /// JIT/GC daemons that share the prefix-free namespace; a genuine per-cycle leak would blow far
  /// past it.
  private static void assertThreadsDidNotAccumulate(final Set<String> before, final ProcessingMode mode)
    throws InterruptedException {
    // Give teardown a moment to let any just-interrupted threads actually exit before the census.
    final var deadline = System.nanoTime() + Duration.ofSeconds(3).toNanos();
    while (System.nanoTime() < deadline) {
      final var leaked = leakedSince(before);
      if (leaked.size() <= 3) break;
      Thread.sleep(50);
    }
    final var leaked = leakedSince(before);
    assertTrue(
      leaked.size() <= 3,
      () ->
        "mode " +
        mode +
        " leaked kpipe/kafka-consumer threads across " +
        CYCLES +
        " cycles: " +
        leaked +
        " (a per-cycle leak would show ~" +
        CYCLES +
        " stranded threads)"
    );
  }

  private static Set<String> leakedSince(final Set<String> before) {
    final var now = kpipeThreadNames();
    now.removeAll(before);
    return now;
  }
}
