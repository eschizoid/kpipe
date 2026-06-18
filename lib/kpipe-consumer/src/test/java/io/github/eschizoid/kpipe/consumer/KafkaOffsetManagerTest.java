package io.github.eschizoid.kpipe.consumer;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.stream.Stream;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.*;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class KafkaOffsetManagerTest {

  private static final String TOPIC = "test-topic";
  private static final TopicPartition PARTITION = new TopicPartition(TOPIC, 0);

  @Mock
  private Consumer<String, byte[]> mockConsumer;

  @Captor
  private ArgumentCaptor<Map<TopicPartition, OffsetAndMetadata>> offsetCaptor;

  private KafkaOffsetManager<String> offsetManager;
  private BlockingQueue<ConsumerCommand> commandQueue;

  @BeforeEach
  void setup() {
    commandQueue = new LinkedBlockingQueue<>();
    offsetManager = KafkaOffsetManager.builder(mockConsumer).withCommandQueue(commandQueue).build();
    offsetManager.start();
  }

  @AfterEach
  void tearDown() {
    if (offsetManager != null) {
      try {
        offsetManager.close();
      } catch (final Exception ignored) {}
      offsetManager = null;
    }

    if (commandQueue != null) {
      commandQueue.clear();
      commandQueue = null;
    }
  }

  @Test
  void shouldHandleEmptyPartitions() throws Exception {
    final var result = offsetManager.commitSyncAndWait(Duration.ofSeconds(1));

    assertTrue(result, "Should handle empty partitions gracefully");
    verify(mockConsumer, never()).commitSync(anyMap());
  }

  @Nested
  class OffsetCommitTests {

    @ParameterizedTest
    @MethodSource("io.github.eschizoid.kpipe.consumer.KafkaOffsetManagerTest#gapSizeScenarios")
    void shouldHandleGapsOfVaryingSizes(
      final String description,
      final long initialOffset,
      final long gapOffset,
      final long expectedOffsetAfterFirstCommit,
      final long expectedOffsetAfterSecondCommit
    ) throws Exception {
      // Initialize with the first record
      final var record = new ConsumerRecord<>(TOPIC, 0, initialOffset, "key", "value".getBytes());
      offsetManager.trackOffset(record);
      offsetManager.markOffsetProcessed(record);

      // First commit
      final var firstCommand = performCommitAndCaptureCommand();

      // Verify the first commit completed successfully
      offsetManager.notifyCommitComplete(firstCommand.commitId(), true);

      // Verify partition state after first commit
      assertEventually(
        () ->
          expectedOffsetAfterFirstCommit == (long) offsetManager.getPartitionState(PARTITION).get("nextOffsetToCommit"),
        Duration.ofSeconds(2),
        "Offset after first commit should be %d".formatted(expectedOffsetAfterFirstCommit)
      );

      // Track and process record with gap offset
      final var largeGapRecord = new ConsumerRecord<>(TOPIC, 0, gapOffset, "key", "value".getBytes());
      offsetManager.trackOffset(largeGapRecord);
      offsetManager.markOffsetProcessed(largeGapRecord);

      // Second commit
      final var secondCommand = performCommitAndCaptureCommand();

      // Complete the second commit
      offsetManager.notifyCommitComplete(secondCommand.commitId(), true);

      // Verify partition state after second commit
      assertEventually(
        () ->
          expectedOffsetAfterSecondCommit ==
          (long) offsetManager.getPartitionState(PARTITION).get("nextOffsetToCommit"),
        Duration.ofSeconds(10),
        "Offset after second commit should be " + expectedOffsetAfterSecondCommit
      );
    }

    @Test
    void shouldHandleCommitByPendingOffsets() throws Exception {
      final var offsetsPending = List.of(1L, 2L, 5L, 6L);
      final var offsetsProcessed = List.of(1L, 2L);
      final var expectedOffsetAfterCommit = 5L;

      offsetsPending.forEach(offset -> {
        final var record = new ConsumerRecord<>(TOPIC, 0, offset, "key", "value".getBytes());
        offsetManager.trackOffset(record);
        if (offsetsProcessed.contains(offset)) {
          offsetManager.markOffsetProcessed(record);
        }
      });

      // commit
      final var firstCommand = performCommitAndCaptureCommand();

      // Verify the commit completed successfully
      offsetManager.notifyCommitComplete(firstCommand.commitId(), true);

      // Verify the partition state after commit
      assertEventually(
        () -> expectedOffsetAfterCommit == (long) offsetManager.getPartitionState(PARTITION).get("nextOffsetToCommit"),
        Duration.ofSeconds(2),
        "Offset after commit should be %d".formatted(expectedOffsetAfterCommit)
      );
    }

    @Test
    void shouldHandleCommitByHighestProcessed() throws Exception {
      final var offsetsPending = List.of(1L, 2L, 3L);
      final var offsetsProcessed = List.of(1L, 2L, 3L);
      final var expectedOffsetAfterCommit = 4L;

      offsetsPending.forEach(offset -> {
        final var record = new ConsumerRecord<>(TOPIC, 0, offset, "key", "value".getBytes());
        offsetManager.trackOffset(record);
        if (offsetsProcessed.contains(offset)) {
          offsetManager.markOffsetProcessed(record);
        }
      });

      // commit
      final var firstCommand = performCommitAndCaptureCommand();

      // Verify the commit completed successfully
      offsetManager.notifyCommitComplete(firstCommand.commitId(), true);

      // Verify the partition state after commit
      assertEventually(
        () -> expectedOffsetAfterCommit == (long) offsetManager.getPartitionState(PARTITION).get("nextOffsetToCommit"),
        Duration.ofSeconds(2),
        "Offset after commit should be %d".formatted(expectedOffsetAfterCommit)
      );
    }
  }

  @Nested
  class RebalanceTests {

    @Test
    void shouldHandleRebalances() {
      // Init with first record
      final var record0 = new ConsumerRecord<>(TOPIC, 0, 100L, "key", "value".getBytes());
      offsetManager.trackOffset(record0);

      // Get rebalance listener
      final var listener = offsetManager.createRebalanceListener();

      // Simulate rebalance
      listener.onPartitionsRevoked(List.of(PARTITION));

      // Verify state cleared after revoke
      final var stateAfterRevoke = offsetManager.getPartitionState(PARTITION);
      assertEquals(
        -1L,
        stateAfterRevoke.get("nextOffsetToCommit"),
        "Next offset to commit should be reset after revoke"
      );

      // Simulate a new partition assignment
      listener.onPartitionsAssigned(List.of(PARTITION));

      // Verify tracking starts over
      final var record1 = new ConsumerRecord<>(TOPIC, 0, 200L, "key", "value".getBytes());
      offsetManager.trackOffset(record1);

      final var stateAfterAssign = offsetManager.getPartitionState(PARTITION);
      assertEquals(
        200L,
        stateAfterAssign.get("nextOffsetToCommit"),
        "Next offset to commit should initialize from first record after assignment"
      );
    }

    @Test
    void shouldCommitCorrectlyAfterPartitionRebalance() {
      // Simulate partition rebalance by clearing and re-tracking
      final var record0 = new ConsumerRecord<>(TOPIC, 0, 101L, "key", "value".getBytes());
      offsetManager.trackOffset(record0);
      offsetManager.markOffsetProcessed(record0);

      try {
        // Track new offsets after rebalance
        var record1 = new ConsumerRecord<>(TOPIC, 0, 501L, "key", "value".getBytes());
        offsetManager.trackOffset(record1);
        offsetManager.markOffsetProcessed(record1);

        // Initiate commit
        CompletableFuture.runAsync(() -> {
          try {
            offsetManager.commitSyncAndWait(Duration.ofSeconds(1));
          } catch (InterruptedException ignored) {}
        });

        // Poll the command and simulate commitSync
        final var command = commandQueue.poll(1, TimeUnit.SECONDS);
        assertNotNull(command, "Should have generated a commit command");
        final var commitCmd = assertInstanceOf(ConsumerCommand.CommitOffsets.class, command);
        mockConsumer.commitSync(commitCmd.offsets());
        offsetManager.notifyCommitComplete(commitCmd.commitId(), true);

        // Verify the correct offset committed
        verify(mockConsumer).commitSync(offsetCaptor.capture());
        assertEquals(
          502L,
          offsetCaptor.getValue().get(PARTITION).offset(),
          "Should commit correct offset after rebalance"
        );
      } catch (final Exception e) {
        fail("Exception during rebalance test: " + e.getMessage());
      }
    }

    /// Revoking only some of this consumer's partitions must clear state for the revoked ones
    /// only and leave the others untouched — otherwise a partial rebalance would corrupt the
    /// surviving partitions' offset bookkeeping.
    @Test
    void shouldHandlePartialRevocation() {
      final var partition2 = new TopicPartition(TOPIC, 2);
      final var recordOnP0 = new ConsumerRecord<>(TOPIC, 0, 110L, "k", "v".getBytes());
      final var recordOnP2 = new ConsumerRecord<>(TOPIC, 2, 310L, "k", "v".getBytes());
      offsetManager.trackOffset(recordOnP0);
      offsetManager.trackOffset(recordOnP2);

      final var listener = offsetManager.createRebalanceListener();
      listener.onPartitionsRevoked(List.of(PARTITION));

      // Revoked partition's state is gone
      assertEquals(-1L, offsetManager.getPartitionState(PARTITION).get("nextOffsetToCommit"));
      // Non-revoked partition is preserved
      assertEquals(310L, offsetManager.getPartitionState(partition2).get("nextOffsetToCommit"));
    }

    /// Pre-existing commit commands targeting a revoked partition would commit against the
    /// wrong owner once the partition is reassigned. The listener prunes them at revoke time.
    @Test
    void shouldDrainCommandQueueForRevokedPartitions() {
      final var partition1 = new TopicPartition(TOPIC, 1);
      final var offsets = Map.of(
        PARTITION,
        new OffsetAndMetadata(100L),
        partition1,
        new OffsetAndMetadata(200L)
      );
      commandQueue.offer(new ConsumerCommand.CommitOffsets(offsets, "commit-1"));

      offsetManager.createRebalanceListener().onPartitionsRevoked(List.of(PARTITION));

      // Command stays, but the revoked-partition entry is filtered out
      assertEquals(1, commandQueue.size());
      final var remaining = assertInstanceOf(ConsumerCommand.CommitOffsets.class, commandQueue.peek());
      assertFalse(remaining.offsets().containsKey(PARTITION), "revoked partition entry must be stripped");
      assertEquals(200L, remaining.offsets().get(partition1).offset(), "surviving partition entry preserved");
    }

    /// Non-commit commands (Pause, Resume, Close, …) don't reference partitions, so they pass
    /// through the drain untouched.
    @Test
    void shouldPreserveNonCommitCommandsDuringRevoke() {
      commandQueue.offer(new ConsumerCommand.Pause());

      offsetManager.createRebalanceListener().onPartitionsRevoked(List.of(PARTITION));

      assertEquals(1, commandQueue.size());
      assertInstanceOf(ConsumerCommand.Pause.class, commandQueue.peek());
    }

    /// If a commit command's offsets are all on revoked partitions, the entire command is
    /// dropped — committing it would target zero partitions, which is a no-op at best and a
    /// brokerwide error at worst depending on the broker version.
    @Test
    void shouldRemoveCommitCommandsThatOnlyTargetRevokedPartitions() {
      commandQueue.offer(new ConsumerCommand.CommitOffsets(Map.of(PARTITION, new OffsetAndMetadata(100L)), "commit-1"));

      offsetManager.createRebalanceListener().onPartitionsRevoked(List.of(PARTITION));

      assertTrue(commandQueue.isEmpty(), "command with only revoked-partition entries must be removed entirely");
    }

    /// After stop(), the listener is a no-op — it must not commit, must not clear state, must
    /// not drain the command queue. Otherwise a late rebalance callback racing with shutdown
    /// would corrupt the post-stop state.
    @Test
    void shouldBeNoOpAfterStop() {
      final var record = new ConsumerRecord<>(TOPIC, 0, 100L, "k", "v".getBytes());
      offsetManager.trackOffset(record);
      commandQueue.offer(new ConsumerCommand.Pause());

      final var listener = offsetManager.createRebalanceListener();
      offsetManager.stop();

      listener.onPartitionsRevoked(List.of(PARTITION));
      listener.onPartitionsAssigned(List.of(PARTITION));

      verify(mockConsumer, never()).commitSync(anyMap());
      assertEquals(1, commandQueue.size(), "command queue must be left alone after stop");
      assertEquals(
        100L,
        offsetManager.getPartitionState(PARTITION).get("nextOffsetToCommit"),
        "partition state must survive a post-stop revoke (regression: would be -1 if revoke ran the clear loop)"
      );
    }

    /// The primary contract of the listener: when partitions are revoked, the highest-processed
    /// offsets (or lowest-pending, if any are still in flight) must be flushed to Kafka via
    /// `commitSync` before the partitions move to the new owner. Without this commit, work the
    /// previous owner already finished would be re-delivered after the rebalance.
    @Test
    void shouldCommitOffsetsWhenPartitionsRevoked() {
      final var record = new ConsumerRecord<>(TOPIC, 0, 100L, "k", "v".getBytes());
      offsetManager.trackOffset(record);
      offsetManager.markOffsetProcessed(record);

      offsetManager.createRebalanceListener().onPartitionsRevoked(List.of(PARTITION));

      verify(mockConsumer).commitSync(offsetCaptor.capture());
      assertEquals(
        101L,
        offsetCaptor.getValue().get(PARTITION).offset(),
        "revoke must flush highestProcessed+1 to commitSync"
      );
    }

    /// If a partition is revoked before any record was tracked or processed, there is nothing to
    /// commit. Calling `commitSync(emptyMap())` is wasted broker traffic at best and a broker
    /// error at worst, so the listener must skip it entirely.
    @Test
    void shouldNotCommitWhenNoOffsetsToCommit() {
      offsetManager.createRebalanceListener().onPartitionsRevoked(List.of(PARTITION));

      verify(mockConsumer, never()).commitSync(anyMap());
    }

    /// The in-flight-at-revoke branch of the revoke flow: when a record was tracked but not yet
    /// marked processed, the listener must commit the lowest-pending offset as the resume point —
    /// not highestProcessed+1. Without this, the partition's new owner would skip the in-flight
    /// record entirely and we'd silently drop work.
    @Test
    void shouldCommitLowestPendingOnRevoke() {
      final var record = new ConsumerRecord<>(TOPIC, 0, 105L, "k", "v".getBytes());
      offsetManager.trackOffset(record);
      // intentionally NOT calling markOffsetProcessed — the record is in flight

      offsetManager.createRebalanceListener().onPartitionsRevoked(List.of(PARTITION));

      verify(mockConsumer).commitSync(offsetCaptor.capture());
      assertEquals(
        105L,
        offsetCaptor.getValue().get(PARTITION).offset(),
        "revoke with an in-flight record must commit the lowest-pending offset as the resume point"
      );
    }

    /// `onPartitionsLost` must still attempt a commit — even though we technically no longer own
    /// the partitions, the commit is the best-effort handoff to the new owner. Pinning that a
    /// commit happens on the lost-path prevents a future "we lost it, skip the commit" refactor
    /// from silently dropping work. (Doesn't pin *delegation* specifically — a copy-pasted commit
    /// body would also pass — and that's fine; the contract is that a commit fires, not how.)
    @Test
    void shouldCommitOnPartitionsLost() {
      final var record = new ConsumerRecord<>(TOPIC, 0, 100L, "k", "v".getBytes());
      offsetManager.trackOffset(record);
      offsetManager.markOffsetProcessed(record);

      offsetManager.createRebalanceListener().onPartitionsLost(List.of(PARTITION));

      verify(mockConsumer).commitSync(offsetCaptor.capture());
      assertEquals(
        101L,
        offsetCaptor.getValue().get(PARTITION).offset(),
        "onPartitionsLost must flush the offsets the previous owner finished"
      );
    }
  }

  /// Guards the single-writer invariant documented on `KafkaOffsetManager.commandQueue`: every
  /// rebalance callback must run on the same thread the listener was first bound to (the Kafka
  /// consumer thread in production). Without this, the poll-then-readd drain in
  /// `drainCommandQueueForRevokedPartitions` becomes racy against concurrent `CommitOffsets`
  /// writers and revoked-partition entries can slip back into the queue.
  ///
  /// Tests in this nested class rely on JVM assertions being enabled (`-ea`). Gradle's `Test`
  /// task enables them by default, so `./gradlew :lib:kpipe-consumer:test` exercises the check.
  @Nested
  class SingleWriterInvariantTests {

    /// Calls the listener from one thread to bind the invariant, then from a different thread
    /// — the second call must trip the `assert` in `assertOnConsumerThread`.
    @Test
    void rebalanceCallbackFromDifferentThreadTripsAssertion() throws Exception {
      final var listener = offsetManager.createRebalanceListener();

      // First call from the JUnit thread captures it as the bound consumer thread.
      listener.onPartitionsRevoked(List.of(PARTITION));

      // Second call from a different thread must trigger the AssertionError.
      final var thrown = new AtomicReference<Throwable>();
      final var off = new Thread(() -> {
        try {
          listener.onPartitionsRevoked(List.of(PARTITION));
        } catch (final Throwable t) {
          thrown.set(t);
        }
      }, "off-thread-rebalancer");
      off.start();
      off.join(2_000);

      final var actual = thrown.get();
      assertNotNull(actual, "off-thread rebalance must throw — assertions disabled?");
      assertInstanceOf(AssertionError.class, actual, "expected AssertionError, got: " + actual);
      assertTrue(
        actual.getMessage().contains("single-writer invariant violated"),
        "assertion message should call out the invariant: " + actual.getMessage()
      );
    }

    /// Repeated callbacks on the same thread must pass — the bound-thread check only fires on a
    /// mismatch.
    @Test
    void repeatedCallbacksOnSameThreadDoNotTrip() {
      final var listener = offsetManager.createRebalanceListener();
      assertDoesNotThrow(() -> {
        listener.onPartitionsRevoked(List.of(PARTITION));
        listener.onPartitionsAssigned(List.of(PARTITION));
        listener.onPartitionsRevoked(List.of(PARTITION));
        listener.onPartitionsLost(List.of(PARTITION));
      });
    }
  }

  @Nested
  class ErrorHandlingTests {

    @Test
    void shouldHandleConsumerExceptions() throws Exception {
      // Track and mark offset as processed
      final var record = new ConsumerRecord<>(TOPIC, 0, 101L, "key", "value".getBytes());
      offsetManager.trackOffset(record);
      offsetManager.markOffsetProcessed(record);

      // Create a future to monitor a commit result
      final var commitFuture = CompletableFuture.supplyAsync(() -> {
        try {
          return offsetManager.commitSyncAndWait(Duration.ofSeconds(1));
        } catch (final InterruptedException e) {
          return false;
        }
      });

      // Get the command
      final var command = pollCommitCommand(Duration.ofSeconds(2));
      assertNotNull(command, "Should generate a commit command");

      // Simulate failure by completing the future with false
      offsetManager.notifyCommitComplete(command.commitId(), false);

      // Assert - the commit should return false but not throw
      final var result = commitFuture.get(2, TimeUnit.SECONDS);
      assertFalse(result, "Commit should report failure");

      // Verify internal state remained consistent
      final var state = offsetManager.getPartitionState(PARTITION);
      assertEquals(101L, state.get("highestProcessedOffset"), "Should maintain highest processed offset");
    }

    @Test
    void shouldHandleConsumerClosedDuringOperation() throws Exception {
      // Track and process an offset
      var record = new ConsumerRecord<>(TOPIC, 0, 101L, "key", "value".getBytes());
      offsetManager.markOffsetProcessed(record);

      // Act - initiate commit
      final var commitFuture = CompletableFuture.supplyAsync(() -> {
        try {
          return offsetManager.commitSyncAndWait(Duration.ofSeconds(1));
        } catch (final InterruptedException e) {
          return false;
        }
      });

      // Get command from the queue
      final var command = pollCommitCommand(Duration.ofSeconds(2));
      assertNotNull(command, "Should have generated a commit command");

      // Simulate consumer closed exception
      offsetManager.notifyCommitComplete(command.commitId(), false);

      // Assert - should handle gracefully
      final var result = commitFuture.get(2, TimeUnit.SECONDS);
      assertFalse(result, "Commit should report failure");

      // Clean up - should not throw
      assertDoesNotThrow(() -> offsetManager.close());
    }

    @Test
    void shouldHandleAsyncCommitFailures() throws Exception {
      // Create a manager with a very short commit interval
      final var asyncManager = KafkaOffsetManager.builder(mockConsumer)
        .withCommandQueue(commandQueue)
        .withCommitInterval(Duration.ofMillis(50))
        .build();

      try {
        // Start the manager to activate the scheduler
        asyncManager.start();

        // Track and process an offset
        final var record = new ConsumerRecord<>(TOPIC, 0, 101L, "key", "value".getBytes());
        asyncManager.trackOffset(record);
        asyncManager.markOffsetProcessed(record);

        // Verify a command was added to the queue
        final var command = pollCommitCommand(Duration.ofSeconds(2));
        assertNotNull(command, "Should have generated a commit command");
        assertInstanceOf(ConsumerCommand.CommitOffsets.class, command, "Command should be of type COMMIT_OFFSETS");

        // Simulate a commit failure
        asyncManager.notifyCommitComplete(command.commitId(), false);
      } finally {
        // Clean up - should not throw even after failure
        assertDoesNotThrow(() -> asyncManager.close());
      }
    }
  }

  @Nested
  class LifecycleTests {

    @Test
    void shouldCloseGracefullyWhenCommitFails() {
      // Track and mark offset
      final var record = new ConsumerRecord<>(TOPIC, 0, 101L, "key", "value".getBytes());
      offsetManager.trackOffset(record);
      offsetManager.markOffsetProcessed(record);

      // Act - close should handle a failed commit
      assertDoesNotThrow(
        () -> {
          final var closeFuture = CompletableFuture.runAsync(offsetManager::close);
          final var command = pollCommitCommand(Duration.ofSeconds(2));
          if (command != null) offsetManager.notifyCommitComplete(command.commitId(), false);
          closeFuture.get(2, TimeUnit.SECONDS);
        },
        "Should close gracefully even when final commit fails"
      );
    }

    @Test
    void shouldHandleRepeatedCloseCalls() {
      // Act - call close multiple times
      offsetManager.close();
      offsetManager.close();
      offsetManager.close();

      // Assert - should not throw exceptions
      verify(mockConsumer, atMostOnce()).commitSync(anyMap());
    }

    @Test
    void shouldInterruptRecoveryOnClose() throws Exception {
      // Act
      final var closedLatch = new CountDownLatch(1);
      final var closed = new AtomicBoolean(false);

      // Start a thread to close in the background
      final var closeThread = new Thread(() -> {
        offsetManager.close();
        closed.set(true);
        closedLatch.countDown();
      });
      closeThread.start();

      // Assert - close should complete even if thread recovery triggered
      assertTrue(closedLatch.await(3, TimeUnit.SECONDS), "Close operation should complete promptly");
      assertTrue(closed.get(), "Should have marked as closed");
    }

    @Test
    void shouldHandleMultipleCloseCalls() {
      // Track and mark an offset so the final-commit path has work to do.
      final var record = new ConsumerRecord<>(TOPIC, 0, 101L, "key", "value".getBytes());
      offsetManager.trackOffset(record);
      offsetManager.markOffsetProcessed(record);

      // First close: invokes kafkaConsumer.commitSync directly with the prepared offsets.
      offsetManager.close();
      verify(mockConsumer, times(1)).commitSync(anyMap(), any(Duration.class));

      // Subsequent close() is a no-op (state already STOPPED) — must not call commitSync again.
      offsetManager.close();
      verify(mockConsumer, times(1)).commitSync(anyMap(), any(Duration.class));
    }
  }

  @Nested
  class AutoCommitTests {

    @Test
    void shouldAutomaticallyCommitAtInterval() throws Exception {
      // Create a manager with a short commit interval
      final var autoCommitManager = KafkaOffsetManager.builder(mockConsumer)
        .withCommandQueue(commandQueue)
        .withCommitInterval(Duration.ofMillis(50))
        .build();

      try {
        // Start the manager to activate the scheduler
        autoCommitManager.start();

        // Track and mark an offset
        final var record = new ConsumerRecord<>(TOPIC, 0, 101L, "key", "value".getBytes());
        autoCommitManager.trackOffset(record);
        autoCommitManager.markOffsetProcessed(record);

        // Verify a command was added to the queue
        final var command = pollCommitCommand(Duration.ofSeconds(2));
        assertNotNull(command, "Should have generated a commit command");
        assertInstanceOf(ConsumerCommand.CommitOffsets.class, command, "Command should be of type COMMIT_OFFSETS");
        assertNotNull(command.offsets(), "Command should contain offsets");
        assertTrue(command.offsets().containsKey(PARTITION), "Command should contain our partition");

        autoCommitManager.notifyCommitComplete(command.commitId(), true);
      } finally {
        autoCommitManager.close();
      }
    }

    @Test
    void shouldHandleCommitSyncWithTimeout() throws Exception {
      // Arrange
      final var record = new ConsumerRecord<>(TOPIC, 0, 101L, "key", "value".getBytes());
      offsetManager.trackOffset(record);
      offsetManager.markOffsetProcessed(record);

      // Start the commit operation asynchronously
      final var commitFuture = CompletableFuture.supplyAsync(() -> {
        try {
          return offsetManager.commitSyncAndWait(Duration.ofMillis(500));
        } catch (final InterruptedException e) {
          return false;
        }
      });

      // Get the commit command from the queue
      final var command = pollCommitCommand(Duration.ofSeconds(2));
      assertNotNull(command, "Should have generated a commit command");

      // Simulate successful commit completion
      offsetManager.notifyCommitComplete(command.commitId(), true);

      // Get the result and verify
      final var result = commitFuture.get(1, TimeUnit.SECONDS);
      assertTrue(result, "Commit should succeed with timeout");

      // Verify the offset was correctly committed
      final var offsets = command.offsets();
      assertEquals(102L, offsets.get(PARTITION).offset(), "Should commit correct offset");
    }
  }

  @Nested
  class EdgeCaseTests {

    @Test
    void shouldHandleTrackingBeforeConsumerPosition() throws Exception {
      // Track a low offset (regardless of consumer position)
      final var record = new ConsumerRecord<>(TOPIC, 0, 99L, "key", "value".getBytes());
      offsetManager.trackOffset(record);
      offsetManager.markOffsetProcessed(record);

      // Act - initiate commit
      final var commitFuture = CompletableFuture.supplyAsync(() -> {
        try {
          return offsetManager.commitSyncAndWait(Duration.ofSeconds(1));
        } catch (final InterruptedException e) {
          return false;
        }
      });

      // Get and verify the command
      final var command = pollCommitCommand(Duration.ofSeconds(2));
      assertNotNull(command, "Should have generated a commit command");

      // Complete the commit successfully
      offsetManager.notifyCommitComplete(command.commitId(), true);

      // Assert
      final var result = commitFuture.get(2, TimeUnit.SECONDS);
      assertTrue(result, "Commit should succeed");

      // Verify the offset in the commit command
      final var offsets = command.offsets();
      assertEquals(100L, offsets.get(PARTITION).offset(), "Should commit exactly the processed offset (99+1)");
    }

    @Test
    void shouldHandleProcessingOfUnknownOffset() {
      // Act - process offset that wasn't tracked
      final var record = new ConsumerRecord<>(TOPIC, 0, 999L, "key", "value".getBytes());
      offsetManager.markOffsetProcessed(record);

      // Assert - should not throw exceptions or cause issues
      assertDoesNotThrow(() -> offsetManager.commitSyncAndWait(Duration.ofSeconds(1)));
    }

    @Test
    void shouldIgnoreDuplicateOffsetTracking() throws Exception {
      // Track the same offset multiple times
      final var record = new ConsumerRecord<>(TOPIC, 0, 101L, "key", "value".getBytes());
      offsetManager.trackOffset(record);
      offsetManager.trackOffset(record); // Duplicate
      offsetManager.markOffsetProcessed(record);

      // Act - initiate commit
      final var commitFuture = CompletableFuture.supplyAsync(() -> {
        try {
          return offsetManager.commitSyncAndWait(Duration.ofSeconds(1));
        } catch (final InterruptedException e) {
          return false;
        }
      });

      // Get and verify command
      final var command = pollCommitCommand(Duration.ofSeconds(2));
      assertNotNull(command, "Should have generated a commit command");
      assertInstanceOf(ConsumerCommand.CommitOffsets.class, command, "Command should be of type COMMIT_OFFSETS");

      // Verify the offsets in the command
      var offsets = command.offsets();
      assertNotNull(offsets, "Command should contain offsets");
      assertTrue(offsets.containsKey(PARTITION), "Command should contain our partition");
      assertEquals(102L, offsets.get(PARTITION).offset(), "Should deduplicate tracked offsets");

      // Complete the commit
      offsetManager.notifyCommitComplete(command.commitId(), true);

      // Assert commit completed successfully
      final var result = commitFuture.get(2, TimeUnit.SECONDS);
      assertTrue(result, "Commit should succeed");
    }
  }

  @Nested
  class MultiPartitionTests {

    @Test
    void shouldHandleMultiplePartitions() throws Exception {
      // Arrange - create multiple partitions
      final var record0 = new ConsumerRecord<>(TOPIC, 0, 101L, "key", "value".getBytes());
      final var record1 = new ConsumerRecord<>(TOPIC, 1, 201L, "key", "value".getBytes());
      final var partition0 = new TopicPartition(TOPIC, 0);
      final var partition1 = new TopicPartition(TOPIC, 1);

      // Track and process offsets for both partitions
      offsetManager.trackOffset(record0);
      offsetManager.trackOffset(record1);
      offsetManager.markOffsetProcessed(record0);
      offsetManager.markOffsetProcessed(record1);

      // Act - use the helper to capture the command from the queue
      final var command = performCommitAndCaptureCommand();

      // Simulate successful commit
      offsetManager.notifyCommitComplete(command.commitId(), true);

      // Assert - check the command's offsets
      final var committedOffsets = command.offsets();
      assertEquals(2, committedOffsets.size(), "Should commit for both partitions");
      assertEquals(102L, committedOffsets.get(partition0).offset(), "Should commit correct offset for partition 0");
      assertEquals(202L, committedOffsets.get(partition1).offset(), "Should commit correct offset for partition 1");
    }
  }

  @Nested
  class ConcurrencyTests {

    @Test
    void shouldHandleConcurrentAccess() throws Exception {
      // Use a manager with a shorter interval for this test
      final var concurrentManager = KafkaOffsetManager.builder(mockConsumer)
        .withCommandQueue(commandQueue)
        .withCommitInterval(Duration.ofMillis(500))
        .build();

      final var threadCount = 4;
      final var offsetsPerThread = 40;
      final var startLatch = new CountDownLatch(1);
      final var completionLatch = new CountDownLatch(threadCount);

      try {
        // Create multiple threads updating offsets
        for (var i = 0; i < threadCount; i++) {
          final int threadId = i;
          new Thread(() -> {
            try {
              startLatch.await(); // Wait for all threads to be ready
              // Each thread processes its own range of offsets
              for (int j = 0; j < offsetsPerThread; j++) {
                final var offset = threadId * offsetsPerThread + j;
                final var record = new ConsumerRecord<>(TOPIC, 0, offset, "key", "value".getBytes());
                concurrentManager.trackOffset(record);
                concurrentManager.markOffsetProcessed(record);
              }
            } catch (final Exception e) {
              fail("Exception in test thread: " + e.getMessage());
            } finally {
              completionLatch.countDown();
            }
          })
            .start();
        }

        // Start all threads simultaneously
        startLatch.countDown();

        // Wait for all threads to complete
        assertTrue(completionLatch.await(3, TimeUnit.SECONDS), "All threads should complete in time");

        // Force final commit and verify
        final var commitFuture = CompletableFuture.supplyAsync(() -> {
          try {
            return concurrentManager.commitSyncAndWait(Duration.ofMillis(500));
          } catch (final InterruptedException e) {
            return false;
          }
        });

        final var command = pollCommitCommand(Duration.ofSeconds(2));
        assertNotNull(command, "Should generate a commit command after concurrent processing");
        concurrentManager.notifyCommitComplete(command.commitId(), true);
        assertTrue(commitFuture.get(2, TimeUnit.SECONDS), "Final commit should complete successfully");
      } finally {
        concurrentManager.close();
      }
    }
  }

  @Nested
  class ConfigurationTests {

    @Test
    void shouldRespectBuilderConfigurations() {
      // Create a manager with custom configuration
      final var record = new ConsumerRecord<>(TOPIC, 0, 101L, "key", "value".getBytes());
      final var customManager = KafkaOffsetManager.builder(mockConsumer)
        .withCommandQueue(commandQueue)
        .withCommitInterval(Duration.ofMillis(200))
        .build();

      // Verify it can be created and closed without exceptions
      assertDoesNotThrow(() -> {
        customManager.trackOffset(record);
        customManager.markOffsetProcessed(record);
        customManager.close();
      });
    }
  }

  private ConsumerCommand.CommitOffsets performCommitAndCaptureCommand() {
    // Start the commit operation and capture the command produced for it.
    CompletableFuture.runAsync(() -> {
      try {
        offsetManager.commitSyncAndWait(Duration.ofSeconds(30));
      } catch (final InterruptedException e) {
        // Ignore interruption
      }
    });

    final var command = pollCommitCommand(Duration.ofSeconds(3));
    assertNotNull(command, "Commit command should be generated");
    return command;
  }

  private ConsumerCommand.CommitOffsets pollCommitCommand(final Duration timeout) {
    try {
      final var command = commandQueue.poll(timeout.toMillis(), TimeUnit.MILLISECONDS);
      return command instanceof ConsumerCommand.CommitOffsets co ? co : null;
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      return null;
    }
  }

  private void assertEventually(final Supplier<Boolean> condition, final Duration timeout, final String message)
    throws Exception {
    final var deadline = System.currentTimeMillis() + timeout.toMillis();

    while (System.currentTimeMillis() < deadline) {
      if (condition.get()) {
        return;
      }
      Thread.sleep(50);
    }

    fail(message);
  }

  static Stream<Arguments> gapSizeScenarios() {
    return Stream.of(
      Arguments.of(
        "Large gap between offsets",
        1_000L, // initial offset
        5_000L, // gap offset
        1_001L, // expected offset after first commit
        5_001L // expected offset after the second commit
      ),
      Arguments.of(
        "Very large gap (thousands of offsets)",
        1000L, // initial offset
        5_000L, // gap offset
        1_001L, // expected offset after first commit
        5_001L // expected offset after the second commit
      ),
      Arguments.of(
        "Small gap between offsets",
        1_000L, // initial offset
        1_001L, // gap offset (just 1 more than initial)
        1_001L, // expected offset after first commit
        1_002L // expected offset after the second commit
      ),
      Arguments.of(
        "Starting from offset zero",
        0L, // initial offset
        1_000L, // gap offset
        1L, // expected offset after first commit
        1_001L // expected offset after the second commit
      )
    );
  }

  /// Locks in the `computeIfPresent` + `safeFirst` race fixes from the audit pass: hammers
  /// `trackOffset` and `markOffsetProcessed` from many virtual threads on the same partition
  /// and asserts that no offsets are silently dropped from the commit map.
  @Nested
  class ConcurrencyStressTests {

    @Test
    void shouldNotDropOffsetsUnderConcurrentTrackAndMark() throws Exception {
      final var manager = KafkaOffsetManager.builder(mockConsumer)
        .withCommandQueue(new LinkedBlockingQueue<>())
        .build();
      manager.start();

      final int threadCount = 32;
      final int offsetsPerThread = 250;
      final var totalOffsets = threadCount * offsetsPerThread;
      final var startGate = new CountDownLatch(1);
      final var doneLatch = new CountDownLatch(threadCount);
      final var errors = new CopyOnWriteArrayList<Throwable>();

      try (final var executor = Executors.newVirtualThreadPerTaskExecutor()) {
        for (int t = 0; t < threadCount; t++) {
          final int threadId = t;
          executor.submit(() -> {
            try {
              startGate.await();
              for (int i = 0; i < offsetsPerThread; i++) {
                final var offset = (long) (threadId * offsetsPerThread + i);
                final var record = new ConsumerRecord<>(TOPIC, 0, offset, "k", "v".getBytes());
                manager.trackOffset(record);
                manager.markOffsetProcessed(record);
              }
            } catch (final Throwable e) {
              errors.add(e);
            } finally {
              doneLatch.countDown();
            }
          });
        }

        startGate.countDown();
        assertTrue(doneLatch.await(15, TimeUnit.SECONDS), "all worker threads should finish in 15s");
      }

      assertTrue(errors.isEmpty(), "no thread should fail: " + errors);

      final var stats = manager.getStatistics();
      assertEquals(0, (int) stats.get("totalPendingOffsets"), "all tracked offsets should be marked");

      // The "highest processed" offset must equal the last offset across all threads.
      final var partitionState = manager.getPartitionState(PARTITION);
      assertEquals(totalOffsets - 1L, partitionState.get("highestProcessedOffset"));
      manager.close();
    }

    @Test
    void prepareOffsetsToCommitShouldNeverThrowUnderRace() throws Exception {
      final var manager = KafkaOffsetManager.builder(mockConsumer)
        .withCommandQueue(new LinkedBlockingQueue<>())
        .build();
      manager.start();

      final var stop = new AtomicBoolean(false);
      final var errors = new CopyOnWriteArrayList<Throwable>();

      try (final var executor = Executors.newVirtualThreadPerTaskExecutor()) {
        // Continuous track / mark churn on partition 0.
        executor.submit(() -> {
          long offset = 0;
          while (!stop.get()) {
            try {
              final var record = new ConsumerRecord<>(TOPIC, 0, offset++, "k", "v".getBytes());
              manager.trackOffset(record);
              manager.markOffsetProcessed(record);
            } catch (final Throwable e) {
              errors.add(e);
              return;
            }
          }
        });

        // Concurrent reader: hammer getPartitionState (also goes through pending.first()).
        for (int i = 0; i < 4; i++) {
          executor.submit(() -> {
            while (!stop.get()) {
              try {
                manager.getPartitionState(PARTITION);
              } catch (final Throwable e) {
                errors.add(e);
                return;
              }
            }
          });
        }

        // Run the race for 500ms — long enough for hundreds of thousands of iterations.
        Thread.sleep(500);
        stop.set(true);
      }

      assertTrue(errors.isEmpty(), "pending.first() race must not throw NoSuchElementException: " + errors);
      manager.close();
    }
  }

  /// Verifies the JLS visibility fix on `private volatile ScheduledExecutorService scheduler` /
  /// `scheduledCommitTask`: a `close()` racing against a concurrent `start()` must always observe
  /// the scheduler reference written by `start()` and shut it down — never leak the
  /// `offset-commit-scheduler` platform thread.
  @Nested
  class SchedulerVisibilityTests {

    /// Races `start()` on one virtual thread against `close()` on another for many iterations.
    /// After every iteration finishes, asserts that no orphan `offset-commit-scheduler` thread
    /// remains alive in the JVM. Without `volatile` on the scheduler field, `close()` could read
    /// a stale `null` and skip `scheduler.shutdown()`, leaving the daemon thread parked forever.
    ///
    /// To exercise the JLS visibility guarantee specifically (without conflating it with the
    /// independent state-machine ordering question of "what if close() CASes before start()
    /// publishes the scheduler reference"), each iteration awaits `start()` returning before
    /// firing `close()` from another thread. The volatile contract is what makes that
    /// cross-thread read of `scheduler` safe.
    @Test
    void concurrentStartAndCloseDoesNotLeakScheduler() throws Exception {
      final var iterations = 20;
      final var errors = new CopyOnWriteArrayList<Throwable>();

      // Baseline: the @BeforeEach manager has its own "offset-commit-scheduler" thread alive.
      // We assert iterations don't increase the count, not that the count is zero.
      final var baselineAliveSchedulers = countAliveSchedulerThreads();

      for (int i = 0; i < iterations; i++) {
        final var manager = KafkaOffsetManager.builder(mockConsumer)
          .withCommandQueue(new LinkedBlockingQueue<>())
          .withCommitInterval(Duration.ofMillis(50))
          .build();

        // start() runs to completion on a virtual thread; close() then races on another.
        // The cross-thread handoff is the moment the volatile semantics matter.
        final var startedLatch = new CountDownLatch(1);
        final var doneLatch = new CountDownLatch(1);

        Thread.ofVirtual().start(() -> {
          try {
            manager.start();
          } catch (final Throwable e) {
            errors.add(e);
          } finally {
            startedLatch.countDown();
          }
        });

        assertTrue(startedLatch.await(5, TimeUnit.SECONDS), "start() should complete in iteration " + i);

        Thread.ofVirtual().start(() -> {
          try {
            manager.close();
          } catch (final Throwable e) {
            errors.add(e);
          } finally {
            doneLatch.countDown();
          }
        });

        assertTrue(doneLatch.await(10, TimeUnit.SECONDS), "close() should complete in iteration " + i);
      }

      assertTrue(errors.isEmpty(), "no exception should escape start/close handoff: " + errors);

      // Load-bearing assertion: scan the live JVM thread set for any thread whose name matches
      // the scheduler factory's name. The count must return to baseline — every scheduler
      // created by `start()` was observed and shut down by `close()`. If the count exceeds
      // baseline, close() failed to observe the volatile scheduler reference.
      assertEventually(
        () -> countAliveSchedulerThreads() <= baselineAliveSchedulers,
        Duration.ofSeconds(10),
        "scheduler thread count must return to baseline (=" +
          baselineAliveSchedulers +
          ") — no orphan offset-commit-scheduler threads after start/close races"
      );
    }

    private static long countAliveSchedulerThreads() {
      return Thread.getAllStackTraces()
        .keySet()
        .stream()
        .filter(t -> "offset-commit-scheduler".equals(t.getName()) && t.isAlive())
        .count();
    }
  }

  /// Verifies that `commitSyncAndWait` honors its timeout and cleans up its
  /// `commitFutures` entry when `notifyCommitComplete` never arrives.
  @Nested
  class CommitTimeoutTests {

    /// When a commit command is enqueued but no `notifyCommitComplete` callback ever fires,
    /// `commitSyncAndWait` must:
    /// 1. Return `false` once the timeout elapses (not block forever).
    /// 2. Remove its `CompletableFuture` from `commitFutures`, so `getStatistics()` reports
    ///    `pendingCommits == 0` afterwards.
    @Test
    void commitSyncAndWaitReturnsFalseWhenNotifyDoesNotArrive() throws Exception {
      // Arrange: track and mark a single offset so commitSyncAndWait actually enqueues a command.
      final var record = new ConsumerRecord<>(TOPIC, 0, 101L, "key", "value".getBytes());
      offsetManager.trackOffset(record);
      offsetManager.markOffsetProcessed(record);

      // Act: invoke commitSyncAndWait on a worker thread with a 1-second timeout.
      final var startNanos = System.nanoTime();
      final var commitFuture = CompletableFuture.supplyAsync(() -> {
        try {
          return offsetManager.commitSyncAndWait(Duration.ofSeconds(1));
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
          return null;
        }
      });

      // Confirm the command was enqueued — but intentionally do NOT call notifyCommitComplete.
      final var command = pollCommitCommand(Duration.ofSeconds(2));
      assertNotNull(command, "commitSyncAndWait should have enqueued a CommitOffsets command");

      // Assert: commitSyncAndWait returns false within ~1.5s of the 1s timeout firing.
      final var result = commitFuture.get(2500, TimeUnit.MILLISECONDS);
      final var elapsedMillis = (System.nanoTime() - startNanos) / 1_000_000L;
      assertEquals(Boolean.FALSE, result, "commitSyncAndWait should return false on timeout");
      assertTrue(
        elapsedMillis >= 900 && elapsedMillis <= 2500,
        "commitSyncAndWait should fire near its 1s timeout, elapsed=" + elapsedMillis + "ms"
      );

      // Assert: future was cleaned up via the finally block in performCommit().
      assertEquals(
        0,
        ((Number) offsetManager.getStatistics().get("pendingCommits")).intValue(),
        "pendingCommits should be 0 after timeout cleanup"
      );
    }
  }
}
