package io.github.eschizoid.kpipe.consumer;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.*;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.Test;

/// Pins the four-phase shutdown ordering and its CAS-once guard.
///
/// `KPipeConsumer.close()` and a self-terminating consumer thread (uncaught throw) can both reach
/// the teardown path. `releaseConstructedResources()` must run exactly once across those racing
/// callers (CAS on an `AtomicBoolean`), and within it the offset manager must be closed — its final
/// commitSync flushes the last safe offsets — while the Kafka consumer is still open, and before
/// the Kafka consumer itself is closed. A second release run would double-commit; closing the Kafka
/// consumer first would make the offset manager's final commitSync throw and silently drop the last
/// offsets.
///
/// The offset manager is the observable probe for the whole release sequence: it is closed inside
/// `releaseConstructedResources()` (so its `close()` count == the number of times release ran) and
/// it sees whether the Kafka consumer is still open at that moment (so it pins the ordering). An
/// externally-supplied producer is deliberately NOT owned/closed by the wrapper, so it is not a
/// usable ordering probe here and is omitted.
class ShutdownOrderingTest {

  private static final String TOPIC = "shutdown-topic";

  private Properties props() {
    final var p = new Properties();
    p.put("bootstrap.servers", "localhost:9092");
    p.put("group.id", "shutdown-group");
    p.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    p.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
    p.put("enable.auto.commit", "false");
    return p;
  }

  private ConsumerRecord<byte[], byte[]> record(final long offset) {
    return new ConsumerRecord<>(TOPIC, 0, offset, ("k" + offset).getBytes(UTF_8), ("v" + offset).getBytes());
  }

  /// A `MockConsumer` whose `subscribe(...)` is a no-op so the test can pre-`assign` partitions and
  /// seed records itself — `MockConsumer` otherwise rejects mixing subscribe + assign. Mirrors the
  /// pattern the existing shutdown tests in `KPipeConsumerTest` use.
  private static MockConsumer<byte[], byte[]> nonSubscribingMock() {
    return new MockConsumer<>("earliest") {
      @Override
      public synchronized void subscribe(final Collection<String> topics) {}

      @Override
      public synchronized void subscribe(final Collection<String> topics, final ConsumerRebalanceListener cb) {}
    };
  }

  /// An [OffsetManager] that records the order of teardown events into a shared log and counts its
  /// own `close()` invocations, so a double-release is observable rather than silently idempotent.
  /// It also captures whether the Kafka consumer was still open at close time (the real
  /// `KafkaOffsetManager.close()` runs a final commitSync that needs it).
  private static final class OrderRecordingOffsetManager implements OffsetManager {

    private final ConcurrentLinkedQueue<String> log;
    private final MockConsumer<byte[], byte[]> consumerProbe;
    final AtomicInteger closeCount = new AtomicInteger(0);
    final AtomicInteger marks = new AtomicInteger(0);
    final AtomicBoolean markedAfterClose = new AtomicBoolean(false);
    final AtomicBoolean consumerOpenWhenClosed = new AtomicBoolean(false);

    OrderRecordingOffsetManager(final ConcurrentLinkedQueue<String> log, final MockConsumer<byte[], byte[]> probe) {
      this.log = log;
      this.consumerProbe = probe;
    }

    @Override
    public OffsetManager start() {
      return this;
    }

    @Override
    public OffsetManager stop() {
      return this;
    }

    @Override
    public void trackOffset(final ConsumerRecord<byte[], byte[]> r) {}

    @Override
    public void markOffsetProcessed(final ConsumerRecord<byte[], byte[]> r) {
      if (closeCount.get() > 0) markedAfterClose.set(true);
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
    public OffsetStatistics getStatistics() {
      return OffsetStatistics.empty();
    }

    @Override
    public void close() {
      if (consumerProbe != null && closeCount.get() == 0) consumerOpenWhenClosed.set(!consumerProbe.closed());
      closeCount.incrementAndGet();
      log.add("offsetManager.close");
    }
  }

  @Test
  void releaseRunsExactlyOnceUnderCloseSelfTerminationRace() throws Exception {
    // The headline race: an external close() and a self-terminating consumer thread (poll throws
    // an Error that escapes the loop's catch(Exception)) both converge on the teardown path. The
    // CAS guard on resourcesReleased must let exactly ONE of them run releaseConstructedResources.
    // Were the guard missing, the offset manager would be closed twice — a double final commitSync,
    // i.e. the double-commit this guard exists to prevent. Start the racers from a barrier so they
    // fire as close together as the scheduler allows, then assert the offset manager closed exactly
    // once and saw the Kafka consumer still open.
    final var log = new ConcurrentLinkedQueue<String>();
    final var tp = new TopicPartition(TOPIC, 0);
    final var pollEntered = new CountDownLatch(1);
    final var poisonPoll = new AtomicBoolean(false);

    // poll() signals the test on first entry, then throws an Error the instant poisonPoll flips —
    // driving the self-termination path concurrently with the external close().
    final var mock = new MockConsumer<byte[], byte[]>("earliest") {
      @Override
      public synchronized void subscribe(final Collection<String> topics) {}

      @Override
      public synchronized void subscribe(final Collection<String> topics, final ConsumerRebalanceListener cb) {}

      @Override
      public synchronized ConsumerRecords<byte[], byte[]> poll(final Duration timeout) {
        pollEntered.countDown();
        if (poisonPoll.get()) throw new Error("simulated self-termination racing external close()");
        return ConsumerRecords.empty();
      }
    };
    mock.assign(List.of(tp));
    mock.updateBeginningOffsets(Map.of(tp, 0L));
    final var manager = new OrderRecordingOffsetManager(log, mock);

    final var consumer = KPipeConsumer.builder()
      .withProperties(props())
      .withTopic(TOPIC)
      .withPipeline(TestPipelines.identity())
      .withConsumer(() -> mock)
      .withOffsetManager(manager)
      .withPollTimeout(Duration.ofMillis(5))
      .withThreadTerminationTimeout(Duration.ofSeconds(2))
      .build();

    consumer.start();
    assertTrue(pollEntered.await(2, TimeUnit.SECONDS), "consumer thread must reach poll() before we race");

    final var barrier = new CyclicBarrier(2);
    final var closer = Thread.ofVirtual().start(() -> {
      try {
        barrier.await();
      } catch (final Exception e) {
        Thread.currentThread().interrupt();
      }
      consumer.close();
    });
    poisonPoll.set(true);
    barrier.await();

    closer.join(TimeUnit.SECONDS.toMillis(5));
    assertTrue(consumer.awaitShutdown(Duration.ofSeconds(5)), "consumer must reach CLOSED");

    assertEquals(
      1,
      manager.closeCount.get(),
      "offsetManager.close() must run exactly once under the close/self-termination race (a second run is a double-commit)"
    );
    assertEquals(
      1,
      log.stream().filter("offsetManager.close"::equals).count(),
      "exactly one offsetManager close event must be logged"
    );
    assertTrue(
      manager.consumerOpenWhenClosed.get(),
      "the single offset-manager close must see the Kafka consumer still open (final commitSync needs it)"
    );
  }

  @Test
  void closeNeverStartedReleasesCtorResourcesExactlyOnceAndIsIdempotent() {
    // The never-started fast path (CREATED -> CLOSED) still runs releaseConstructedResources()
    // (which closes the offset manager) and then closes the Kafka consumer directly, because the
    // consumer thread that normally owns that close never ran. The offset manager must be closed
    // while the Kafka consumer is still open (ordering holds even on the fast path). A second
    // close() must be a clean no-op: the state CAS already moved past CREATED, so nothing is closed
    // twice.
    final var log = new ConcurrentLinkedQueue<String>();
    final var mock = nonSubscribingMock();
    final var manager = new OrderRecordingOffsetManager(log, mock);

    final var consumer = KPipeConsumer.builder()
      .withProperties(props())
      .withTopic(TOPIC)
      .withPipeline(TestPipelines.identity())
      .withConsumer(() -> mock)
      .withOffsetManager(manager)
      .build();

    consumer.close(); // never started -> fast path
    consumer.close(); // idempotent -> must not re-release

    assertEquals(1, manager.closeCount.get(), "never-started close must close the offset manager exactly once");
    assertTrue(mock.closed(), "never-started close must close the underlying Kafka consumer");
    assertTrue(
      manager.consumerOpenWhenClosed.get(),
      "the offset manager must close (final commitSync) while the Kafka consumer is still open"
    );
    assertFalse(consumer.isRunning());
  }

  @Test
  void shutdownDrainsInFlightMarkBeforeClosingOffsetManager() throws Exception {
    // On a normal close() the poll loop exits the instant state becomes CLOSING, but a record still
    // in flight (PARALLEL, slow pipeline) finishes afterward and marks its offset. The drain must
    // wait for that mark, and it must land BEFORE the offset manager is closed — otherwise the last
    // offset is lost (at-least-once breaks, the record reprocesses on restart). And the offset
    // manager must close exactly once while the Kafka consumer is still open.
    final var log = new ConcurrentLinkedQueue<String>();
    final var mock = nonSubscribingMock();
    final var tp = new TopicPartition(TOPIC, 0);
    mock.assign(List.of(tp));
    mock.updateBeginningOffsets(Map.of(tp, 0L));
    mock.addRecord(record(0));

    final var manager = new OrderRecordingOffsetManager(log, mock);
    final var entered = new CountDownLatch(1);

    final var consumer = KPipeConsumer.builder()
      .withProperties(props())
      .withTopic(TOPIC)
      .withProcessingMode(ProcessingMode.PARALLEL)
      .withPipeline(
        TestPipelines.sideEffect(v -> {
          entered.countDown();
          // Still in flight when close() fires, forcing the drain to wait on this
          // record.
          try {
            Thread.sleep(150);
          } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
          }
          return v;
        })
      )
      .withConsumer(() -> mock)
      .withOffsetManager(manager)
      .withPollTimeout(Duration.ofMillis(10))
      .withWaitForMessagesTimeout(Duration.ofSeconds(2))
      .withThreadTerminationTimeout(Duration.ofSeconds(2))
      .build();

    consumer.start();
    assertTrue(entered.await(2, TimeUnit.SECONDS), "the record must reach the pipeline before we close");

    consumer.close();
    assertTrue(consumer.awaitShutdown(Duration.ofSeconds(5)), "consumer must reach CLOSED");

    assertTrue(
      manager.marks.get() >= 1,
      "the in-flight record's offset must be marked during the drain, not abandoned"
    );
    assertFalse(
      manager.markedAfterClose.get(),
      "the in-flight offset must be marked BEFORE the offset manager is closed (else it is silently lost)"
    );
    assertEquals(1, manager.closeCount.get(), "offset manager closed exactly once on the normal path");
    assertTrue(
      manager.consumerOpenWhenClosed.get(),
      "offset manager must close (final commitSync) while the Kafka consumer is still open"
    );
  }
}
