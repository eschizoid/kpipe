package io.github.eschizoid.kpipe.consumer;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.io.IOException;
import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/// [KafkaOffsetManager] with an [OffsetStore] in place of Kafka.
class KafkaOffsetManagerStoreTest {

  private static final String TOPIC = "store-topic";
  private static final TopicPartition PARTITION = new TopicPartition(TOPIC, 0);

  @SuppressWarnings("unchecked")
  private final Consumer<byte[], byte[]> consumer = mock(Consumer.class);

  private final FakeStore store = new FakeStore();
  private KafkaOffsetManager manager;

  @AfterEach
  void tearDown() {
    if (manager != null) manager.close();
  }

  /// A store that keeps every commit and can be told to fail its next loads or commits.
  static final class FakeStore implements OffsetStore {

    final List<Map<TopicPartition, Long>> commits = new CopyOnWriteArrayList<>();
    final Map<TopicPartition, Long> stored = new ConcurrentHashMap<>();
    final AtomicInteger failingLoads = new AtomicInteger();
    final AtomicInteger checkedLoadFailures = new AtomicInteger();
    final AtomicInteger failingCommits = new AtomicInteger();
    final AtomicInteger loads = new AtomicInteger();
    final AtomicInteger negativeLoads = new AtomicInteger();
    final Map<TopicPartition, Long> extra = new ConcurrentHashMap<>();

    @Override
    public Map<TopicPartition, Long> load(final Collection<TopicPartition> partitions) {
      loads.incrementAndGet();
      if (checkedLoadFailures.getAndUpdate(n -> Math.max(0, n - 1)) > 0) throwUnchecked(new IOException("store down"));
      if (failingLoads.getAndUpdate(n -> Math.max(0, n - 1)) > 0) throw new IllegalStateException("store down");
      final var result = new HashMap<TopicPartition, Long>(extra);
      if (negativeLoads.getAndUpdate(n -> Math.max(0, n - 1)) > 0) {
        partitions.forEach(partition -> result.put(partition, -1L));
        return result;
      }
      partitions.forEach(partition -> {
        final var offset = stored.get(partition);
        if (offset != null) result.put(partition, offset);
      });
      return result;
    }

    @Override
    public void commit(final Map<TopicPartition, Long> offsets) {
      if (failingCommits.getAndUpdate(n -> Math.max(0, n - 1)) > 0) throw new IllegalStateException("store down");
      commits.add(Map.copyOf(offsets));
      stored.putAll(offsets);
    }
  }

  /// Throws a checked exception from a method that does not declare it, as a store written in
  /// another JVM language can.
  @SuppressWarnings("unchecked")
  private static <E extends Throwable> void throwUnchecked(final Throwable t) throws E {
    throw (E) t;
  }

  private KafkaOffsetManager startManager() {
    manager = KafkaOffsetManager.builder(consumer)
      .withOffsetStore(store)
      .withCommitInterval(Duration.ofHours(1))
      .build();
    manager.start();
    return manager;
  }

  private static ConsumerRecord<byte[], byte[]> record(final long offset) {
    return new ConsumerRecord<>(TOPIC, 0, offset, new byte[0], new byte[0]);
  }

  @Test
  void assignmentSeeksToTheStoredOffset() {
    store.stored.put(PARTITION, 42L);
    startManager().createRebalanceListener().onPartitionsAssigned(List.of(PARTITION));

    verify(consumer).seek(PARTITION, 42L);
  }

  @Test
  void aPartitionWithoutAStoredOffsetIsNotSought() {
    startManager().createRebalanceListener().onPartitionsAssigned(List.of(PARTITION));

    verify(consumer, never()).seek(any(TopicPartition.class), anyLong());
  }

  @Test
  void aFailedLoadIsRetriedUntilItSucceeds() {
    store.stored.put(PARTITION, 7L);
    store.failingLoads.set(2);
    startManager().createRebalanceListener().onPartitionsAssigned(List.of(PARTITION));

    assertEquals(3, store.loads.get(), "two failed loads, then one that succeeds");
    verify(consumer).seek(PARTITION, 7L);
  }

  /// A store that answers with an offset no partition can hold is retried, and the partition is
  /// never sought to it.
  @Test
  void aLoadWithANegativeOffsetIsRetriedAndNeverApplied() {
    store.stored.put(PARTITION, 7L);
    store.negativeLoads.set(2);
    startManager().createRebalanceListener().onPartitionsAssigned(List.of(PARTITION));

    assertEquals(3, store.loads.get(), "two invalid loads, then a valid one");
    verify(consumer, never()).seek(PARTITION, -1L);
    verify(consumer).seek(PARTITION, 7L);
  }

  @Test
  void anOffsetForAPartitionThatWasNotRequestedIsIgnored() {
    final var other = new TopicPartition(TOPIC, 9);
    store.stored.put(PARTITION, 7L);
    store.extra.put(other, 3L);
    startManager().createRebalanceListener().onPartitionsAssigned(List.of(PARTITION));

    verify(consumer).seek(PARTITION, 7L);
    verify(consumer, never()).seek(other, 3L);
  }

  /// Every commit outcome reaches the observer: a failed write, then a successful one.
  @Test
  void eachCommitOutcomeIsObserved() throws Exception {
    final var committed = new AtomicInteger();
    final var failed = new AtomicInteger();
    startManager().observeCommits(
      new CommitObserver() {
        @Override
        public void committed() {
          committed.incrementAndGet();
        }

        @Override
        public void failed() {
          failed.incrementAndGet();
        }
      }
    );
    manager.trackOffset(record(0));
    manager.markOffsetProcessed(record(0));
    store.failingCommits.set(1);

    assertFalse(manager.commitSyncAndWait(Duration.ofSeconds(1)));
    assertTrue(manager.commitSyncAndWait(Duration.ofSeconds(1)));
    assertEquals(1, failed.get());
    assertEquals(1, committed.get());
  }

  @Test
  void aCheckedExceptionFromLoadIsRetriedLikeAnyOther() {
    store.stored.put(PARTITION, 7L);
    store.checkedLoadFailures.set(1);
    startManager().createRebalanceListener().onPartitionsAssigned(List.of(PARTITION));

    assertEquals(2, store.loads.get(), "one load throwing IOException, then one that succeeds");
    verify(consumer).seek(PARTITION, 7L);
  }

  @Test
  void signalClosingEndsTheWaitBetweenLoadRetriesAtOnce() throws Exception {
    store.failingLoads.set(Integer.MAX_VALUE);
    final var listener = startManager().createRebalanceListener();
    final var assignment = CompletableFuture.runAsync(() -> listener.onPartitionsAssigned(List.of(PARTITION)));
    while (store.loads.get() < 4) TimeUnit.MILLISECONDS.sleep(10);
    final var signalled = System.nanoTime();
    manager.signalClosing();

    assertThrows(ExecutionException.class, () -> assignment.get(10, TimeUnit.SECONDS));
    final var waited = Duration.ofNanos(System.nanoTime() - signalled);
    assertTrue(waited.compareTo(Duration.ofMillis(500)) < 0, "the retry wait ends on the signal; took " + waited);
    verify(consumer, never()).seek(any(TopicPartition.class), anyLong());
  }

  @Test
  void aLostPartitionIsForgottenWithoutAStoreWrite() throws Exception {
    final var listener = startManager().createRebalanceListener();
    manager.trackOffset(record(0));
    manager.markOffsetProcessed(record(0));

    listener.onPartitionsLost(List.of(PARTITION));

    assertTrue(store.commits.isEmpty(), "a lost partition may already belong to another consumer");
    assertTrue(manager.commitSyncAndWait(Duration.ofSeconds(1)));
    assertTrue(store.commits.isEmpty(), "the lost partition's state is gone, so later commits skip it");
  }

  /// A periodic commit that is still writing when a revoke arrives finishes first; the revoke then
  /// writes the final frontier, so the last write for the partition is the revoke's.
  @Test
  void aRevokeWaitsForAPeriodicCommitThatIsStillWriting() throws Exception {
    final var entered = new CountDownLatch(1);
    final var release = new CountDownLatch(1);
    final var writers = new CopyOnWriteArrayList<String>();
    final OffsetStore blocking = new OffsetStore() {
      @Override
      public Map<TopicPartition, Long> load(final Collection<TopicPartition> partitions) {
        return Map.of();
      }

      @Override
      public void commit(final Map<TopicPartition, Long> offsets) {
        if (writers.isEmpty()) {
          entered.countDown();
          try {
            release.await();
          } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
          }
        }
        writers.add(Thread.currentThread().getName() + offsets);
      }
    };
    manager = KafkaOffsetManager.builder(consumer)
      .withOffsetStore(blocking)
      .withCommitInterval(Duration.ofHours(1))
      .build();
    manager.start();
    final var listener = manager.createRebalanceListener();
    manager.trackOffset(record(0));
    manager.trackOffset(record(1));
    manager.markOffsetProcessed(record(0));

    final var periodic = Thread.ofPlatform().name("periodic").start(manager::commitSafeOffsets);
    assertTrue(entered.await(5, TimeUnit.SECONDS));
    manager.markOffsetProcessed(record(1));
    final var revoke = Thread.ofPlatform()
      .name("revoke")
      .start(() -> listener.onPartitionsRevoked(List.of(PARTITION)));
    revoke.join(300);
    assertTrue(revoke.isAlive(), "the revoke waits while the periodic commit is writing");
    release.countDown();
    periodic.join(5_000);
    revoke.join(5_000);

    assertEquals(List.of("periodic{" + PARTITION + "=1}", "revoke{" + PARTITION + "=2}"), writers);
  }

  @Test
  void aLoadThatNeverSucceedsStopsRetryingWhenTheManagerCloses() throws Exception {
    store.failingLoads.set(Integer.MAX_VALUE);
    final var listener = startManager().createRebalanceListener();
    final var assignment = CompletableFuture.runAsync(() -> listener.onPartitionsAssigned(List.of(PARTITION)));
    while (store.loads.get() < 2) TimeUnit.MILLISECONDS.sleep(10);
    manager.close();

    final var thrown = assertThrows(ExecutionException.class, () -> assignment.get(10, TimeUnit.SECONDS));
    assertInstanceOf(IllegalStateException.class, thrown.getCause());
    verify(consumer, never()).seek(any(TopicPartition.class), anyLong());
  }

  @Test
  void commitsTheFrontierNotTheHighestProcessedOffset() throws Exception {
    startManager();
    for (long offset = 0; offset < 4; offset++) manager.trackOffset(record(offset));
    manager.markOffsetProcessed(record(0));
    manager.markOffsetProcessed(record(2));
    manager.markOffsetProcessed(record(3));

    assertTrue(manager.commitSyncAndWait(Duration.ofSeconds(1)));
    assertEquals(List.of(Map.of(PARTITION, 1L)), store.commits, "offset 1 is still running");
  }

  @Test
  void aFailedCommitReturnsFalseAndTheNextCommitCarriesTheCurrentFrontier() throws Exception {
    startManager();
    manager.trackOffset(record(0));
    manager.markOffsetProcessed(record(0));
    store.failingCommits.set(1);

    assertFalse(manager.commitSyncAndWait(Duration.ofSeconds(1)));
    manager.trackOffset(record(1));
    manager.markOffsetProcessed(record(1));
    assertTrue(manager.commitSyncAndWait(Duration.ofSeconds(1)));
    assertEquals(List.of(Map.of(PARTITION, 2L)), store.commits);
  }

  @Test
  void revokeWritesTheFrontierToTheStoreAndNotToKafka() {
    final var listener = startManager().createRebalanceListener();
    manager.trackOffset(record(0));
    manager.trackOffset(record(1));
    manager.markOffsetProcessed(record(0));

    listener.onPartitionsRevoked(List.of(PARTITION));

    assertEquals(List.of(Map.of(PARTITION, 1L)), store.commits);
    verify(consumer, never()).commitSync(anyMap());
  }

  @Test
  void closeWritesTheFinalFrontierToTheStore() {
    startManager();
    manager.trackOffset(record(0));
    manager.markOffsetProcessed(record(0));
    manager.close();

    assertEquals(List.of(Map.of(PARTITION, 1L)), store.commits);
    verify(consumer, never()).commitSync(anyMap(), any(Duration.class));
  }

  @Test
  void aCommitIntervalUnderAMillisecondIsRejected() {
    final var builder = KafkaOffsetManager.builder(consumer).withOffsetStore(store);
    assertThrows(IllegalArgumentException.class, () -> builder.withCommitInterval(Duration.ofNanos(500_000)));
  }

  @Test
  void buildWithoutACommitExecutorOrAStoreIsRejected() {
    assertThrows(IllegalStateException.class, () -> KafkaOffsetManager.builder(consumer).build());
  }

  @Test
  void theConsumerBuilderRejectsAStoreAlongsideACustomOffsetManager() {
    final var custom = KafkaOffsetManager.builder(consumer).withOffsetStore(store).build();
    final var builder = KPipeConsumer.builder()
      .withProperties(new Properties())
      .withTopic(TOPIC)
      .withPipeline(TestPipelines.identity())
      .withConsumer(() -> consumer)
      .withOffsetManager(custom)
      .withOffsetStore(new FakeStore());

    final var thrown = assertThrows(IllegalArgumentException.class, builder::build);
    assertTrue(thrown.getMessage().contains("withOffsetStore"), thrown.getMessage());
    custom.close();
  }

  @Test
  void anOffsetManagerAndAProviderTogetherAreRejected() {
    final var custom = KafkaOffsetManager.builder(consumer).withOffsetStore(store).build();
    final var builder = KPipeConsumer.builder()
      .withProperties(new Properties())
      .withTopic(TOPIC)
      .withPipeline(TestPipelines.identity())
      .withConsumer(() -> consumer)
      .withOffsetManager(custom)
      .withOffsetManagerProvider(c -> KafkaOffsetManager.builder(c).withOffsetStore(store).build());

    final var thrown = assertThrows(IllegalArgumentException.class, builder::build);
    assertTrue(thrown.getMessage().contains("alternatives"), thrown.getMessage());
    custom.close();
  }

  /// One manager instance cannot serve two consumers: closing the first would stop the second's
  /// commits.
  @Test
  void aSecondBuildWithTheSameOffsetManagerInstanceIsRejected() {
    final var custom = KafkaOffsetManager.builder(consumer).withOffsetStore(store).build();
    final var props = new Properties();
    props.put("bootstrap.servers", "localhost:9092");
    props.put("group.id", "store");
    final var builder = KPipeConsumer.builder()
      .withProperties(props)
      .withTopic(TOPIC)
      .withPipeline(TestPipelines.identity())
      .withConsumer(() -> consumer)
      .withOffsetManager(custom);
    try (final var first = builder.build()) {
      final var thrown = assertThrows(IllegalStateException.class, builder::build);
      assertTrue(thrown.getMessage().contains("withOffsetManagerProvider"), thrown.getMessage());
      assertEquals(0L, first.metrics().records().received(), "the first consumer was built and is usable");
    }
  }
}
