package org.kpipe.consumer;

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
import org.kpipe.consumer.enums.ConsumerCommand;
import org.mockito.*;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class OffsetManagerTest {

  private static final String TOPIC = "test-topic";
  private static final TopicPartition PARTITION = new TopicPartition(TOPIC, 0);

  @Mock
  private Consumer<String, String> mockConsumer;

  @Captor
  private ArgumentCaptor<Map<TopicPartition, OffsetAndMetadata>> offsetCaptor;

  private OffsetManager<String, String> offsetManager;
  private BlockingQueue<ConsumerCommand> commandQueue;

  @BeforeEach
  void setup() {
    commandQueue = new LinkedBlockingQueue<>();
    offsetManager = OffsetManager.builder(mockConsumer).withCommandQueue(commandQueue).build();
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
    final var result = offsetManager.commitSyncAndWait(1);

    assertTrue(result, "Should handle empty partitions gracefully");
    verify(mockConsumer, never()).commitSync(anyMap());
  }

  @Nested
  class OffsetCommitTests {

    @ParameterizedTest
    @MethodSource("org.kpipe.consumer.OffsetManagerTest#gapSizeScenarios")
    void shouldHandleGapsOfVaryingSizes(
      final String description,
      final long initialOffset,
      final long gapOffset,
      final long expectedOffsetAfterFirstCommit,
      final long expectedOffsetAfterSecondCommit
    ) throws Exception {
      // Initialize with the first record
      final var record = new ConsumerRecord<>(TOPIC, 0, initialOffset, "key", "value");
      offsetManager.trackOffset(record);
      offsetManager.markOffsetProcessed(record);

      // First commit
      final var firstCommand = performCommitAndCaptureCommand();

      // Verify the first commit completed successfully
      offsetManager.notifyCommitComplete(firstCommand.getCommitId(), true);

      // Verify partition state after first commit
      assertEventually(
        () ->
          expectedOffsetAfterFirstCommit == (long) offsetManager.getPartitionState(PARTITION).get("nextOffsetToCommit"),
        Duration.ofSeconds(2),
        "Offset after first commit should be %d".formatted(expectedOffsetAfterFirstCommit)
      );

      // Track and process record with gap offset
      final var largeGapRecord = new ConsumerRecord<>(TOPIC, 0, gapOffset, "key", "value");
      offsetManager.trackOffset(largeGapRecord);
      offsetManager.markOffsetProcessed(largeGapRecord);

      // Second commit
      final var secondCommand = performCommitAndCaptureCommand();

      // Complete the second commit
      offsetManager.notifyCommitComplete(secondCommand.getCommitId(), true);

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
        final var record = new ConsumerRecord<>(TOPIC, 0, offset, "key", "value");
        offsetManager.trackOffset(record);
        if (offsetsProcessed.contains(offset)) {
          offsetManager.markOffsetProcessed(record);
        }
      });

      // commit
      final var firstCommand = performCommitAndCaptureCommand();

      // Verify the commit completed successfully
      offsetManager.notifyCommitComplete(firstCommand.getCommitId(), true);

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
        final var record = new ConsumerRecord<>(TOPIC, 0, offset, "key", "value");
        offsetManager.trackOffset(record);
        if (offsetsProcessed.contains(offset)) {
          offsetManager.markOffsetProcessed(record);
        }
      });

      // commit
      final var firstCommand = performCommitAndCaptureCommand();

      // Verify the commit completed successfully
      offsetManager.notifyCommitComplete(firstCommand.getCommitId(), true);

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
      final var record0 = new ConsumerRecord<>(TOPIC, 0, 100L, "key", "value");
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
      final var record1 = new ConsumerRecord<>(TOPIC, 0, 200L, "key", "value");
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
      final var record0 = new ConsumerRecord<>(TOPIC, 0, 101L, "key", "value");
      offsetManager.trackOffset(record0);
      offsetManager.markOffsetProcessed(record0);

      try {
        // Track new offsets after rebalance
        var record1 = new ConsumerRecord<>(TOPIC, 0, 501L, "key", "value");
        offsetManager.trackOffset(record1);
        offsetManager.markOffsetProcessed(record1);

        // Initiate commit
        CompletableFuture.runAsync(() -> {
          try {
            offsetManager.commitSyncAndWait(1);
          } catch (InterruptedException ignored) {}
        });

        // Poll the command and simulate commitSync
        final var command = commandQueue.poll(1, TimeUnit.SECONDS);
        assertNotNull(command, "Should have generated a commit command");
        mockConsumer.commitSync(command.getOffsets());
        offsetManager.notifyCommitComplete(command.getCommitId(), true);

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
  }

  @Nested
  class ErrorHandlingTests {

    @Test
    void shouldHandleConsumerExceptions() throws Exception {
      // Track and mark offset as processed
      final var record = new ConsumerRecord<>(TOPIC, 0, 101L, "key", "value");
      offsetManager.trackOffset(record);
      offsetManager.markOffsetProcessed(record);

      // Create a future to monitor a commit result
      final var commitFuture = CompletableFuture.supplyAsync(() -> {
        try {
          return offsetManager.commitSyncAndWait(1);
        } catch (final InterruptedException e) {
          return false;
        }
      });

      // Wait for the command to be generated
      Thread.sleep(100);

      // Get the command
      final var command = commandQueue.poll();
      assertNotNull(command, "Should generate a commit command");

      // Simulate failure by completing the future with false
      offsetManager.notifyCommitComplete(command.getCommitId(), false);

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
      var record = new ConsumerRecord<>(TOPIC, 0, 101L, "key", "value");
      offsetManager.markOffsetProcessed(record);

      // Act - initiate commit
      final var commitFuture = CompletableFuture.supplyAsync(() -> {
        try {
          return offsetManager.commitSyncAndWait(1);
        } catch (final InterruptedException e) {
          return false;
        }
      });

      // Wait for the command to be generated
      Thread.sleep(100);

      // Get command from the queue
      final var command = commandQueue.poll();
      assertNotNull(command, "Should have generated a commit command");

      // Simulate consumer closed exception
      offsetManager.notifyCommitComplete(command.getCommitId(), false);

      // Assert - should handle gracefully
      final var result = commitFuture.get(2, TimeUnit.SECONDS);
      assertFalse(result, "Commit should report failure");

      // Clean up - should not throw
      assertDoesNotThrow(() -> offsetManager.close());
    }

    @Test
    void shouldHandleAsyncCommitFailures() throws Exception {
      // Create a manager with a very short commit interval
      final var asyncManager = OffsetManager
        .builder(mockConsumer)
        .withCommandQueue(commandQueue)
        .withCommitInterval(Duration.ofMillis(50))
        .build();

      // Start the manager to activate the scheduler
      asyncManager.start();

      // Track and process an offset
      final var record = new ConsumerRecord<>(TOPIC, 0, 101L, "key", "value");
      asyncManager.trackOffset(record);
      asyncManager.markOffsetProcessed(record);

      // Wait for auto-commit to occur
      Thread.sleep(200);

      // Verify a command was added to the queue
      final var command = commandQueue.poll(1000, TimeUnit.MILLISECONDS);
      assertNotNull(command, "Should have generated a commit command");
      assertEquals(ConsumerCommand.COMMIT_OFFSETS, command, "Command should be of type COMMIT_OFFSETS");

      // Simulate a commit failure
      asyncManager.notifyCommitComplete(command.getCommitId(), false);

      // Clean up - should not throw even after failure
      assertDoesNotThrow(asyncManager::close);

      // Get and complete the final commit command during close
      final var closeCommand = commandQueue.poll(1000, TimeUnit.MILLISECONDS);
      if (closeCommand != null) {
        asyncManager.notifyCommitComplete(closeCommand.getCommitId(), true);
      }
    }
  }

  @Nested
  class LifecycleTests {

    @Test
    void shouldCloseGracefullyWhenCommitFails() {
      // Track and mark offset
      final var record = new ConsumerRecord<>(TOPIC, 0, 101L, "key", "value");
      offsetManager.trackOffset(record);
      offsetManager.markOffsetProcessed(record);

      // Act - close should handle a failed commit
      assertDoesNotThrow(
        () -> {
          CompletableFuture.runAsync(offsetManager::close);
          Thread.sleep(100);
          final var command = commandQueue.poll();
          if (command != null) {
            offsetManager.notifyCommitComplete(command.getCommitId(), false);
          }
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
    void shouldHandleMultipleCloseCalls() throws Exception {
      // Track and mark an offset
      final var record = new ConsumerRecord<>(TOPIC, 0, 101L, "key", "value");
      offsetManager.trackOffset(record);
      offsetManager.markOffsetProcessed(record);
      Thread.sleep(100);

      // First close - run asynchronously so we can capture the command
      CompletableFuture.runAsync(offsetManager::close);

      // Wait for the command to be added to the queue
      Thread.sleep(100);

      // Verify a command was added to the queue
      final var command = commandQueue.poll(1000, TimeUnit.MILLISECONDS);
      assertNotNull(command, "Should have generated a commit command during close");
      assertEquals(ConsumerCommand.COMMIT_OFFSETS, command, "Command should be of type COMMIT_OFFSETS");

      // Simulate successful commit completion
      offsetManager.notifyCommitComplete(command.getCommitId(), true);

      // Wait for close to complete
      Thread.sleep(100);

      // A second close should be safe
      offsetManager.close();

      // No additional commands should be in the queue
      assertNull(commandQueue.poll(), "No additional commands should be added on second close");
    }
  }

  @Nested
  class AutoCommitTests {

    @Test
    void shouldAutomaticallyCommitAtInterval() throws Exception {
      // Create a manager with a short commit interval
      final var autoCommitManager = OffsetManager
        .builder(mockConsumer)
        .withCommandQueue(commandQueue)
        .withCommitInterval(Duration.ofMillis(50))
        .build();

      // Start the manager to activate the scheduler
      autoCommitManager.start();

      // Track and mark an offset
      final var record = new ConsumerRecord<>(TOPIC, 0, 101L, "key", "value");
      autoCommitManager.trackOffset(record);
      autoCommitManager.markOffsetProcessed(record);

      // Wait for auto-commit to occur
      Thread.sleep(200);

      // Verify a command was added to the queue
      final var command = commandQueue.poll(1000, TimeUnit.MILLISECONDS);
      assertNotNull(command, "Should have generated a commit command");
      assertEquals(ConsumerCommand.COMMIT_OFFSETS, command, "Command should be of type COMMIT_OFFSETS");
      assertNotNull(command.getOffsets(), "Command should contain offsets");
      assertTrue(command.getOffsets().containsKey(PARTITION), "Command should contain our partition");
    }

    @Test
    void shouldHandleCommitSyncWithTimeout() throws Exception {
      // Arrange
      final var record = new ConsumerRecord<>(TOPIC, 0, 101L, "key", "value");
      offsetManager.trackOffset(record);
      offsetManager.markOffsetProcessed(record);

      // Start the commit operation asynchronously
      final var commitFuture = CompletableFuture.supplyAsync(() -> {
        try {
          return offsetManager.commitSyncAndWait(500);
        } catch (final InterruptedException e) {
          return false;
        }
      });

      // Wait for command to be generated
      Thread.sleep(100);

      // Get the commit command from the queue
      final var command = commandQueue.poll(1000, TimeUnit.MILLISECONDS);
      assertNotNull(command, "Should have generated a commit command");

      // Simulate successful commit completion
      offsetManager.notifyCommitComplete(command.getCommitId(), true);

      // Get the result and verify
      final var result = commitFuture.get(1, TimeUnit.SECONDS);
      assertTrue(result, "Commit should succeed with timeout");

      // Verify the offset was correctly committed
      final var offsets = command.getOffsets();
      assertEquals(102L, offsets.get(PARTITION).offset(), "Should commit correct offset");
    }
  }

  @Nested
  class EdgeCaseTests {

    @Test
    void shouldHandleTrackingBeforeConsumerPosition() throws Exception {
      // Track a low offset (regardless of consumer position)
      final var record = new ConsumerRecord<>(TOPIC, 0, 99L, "key", "value");
      offsetManager.trackOffset(record);
      offsetManager.markOffsetProcessed(record);

      // Act - initiate commit
      final var commitFuture = CompletableFuture.supplyAsync(() -> {
        try {
          return offsetManager.commitSyncAndWait(1);
        } catch (final InterruptedException e) {
          return false;
        }
      });

      // Wait for the command to be generated
      Thread.sleep(100);

      // Get and verify the command
      final var command = commandQueue.poll();
      assertNotNull(command, "Should have generated a commit command");

      // Complete the commit successfully
      offsetManager.notifyCommitComplete(command.getCommitId(), true);

      // Assert
      final var result = commitFuture.get(2, TimeUnit.SECONDS);
      assertTrue(result, "Commit should succeed");

      // Verify the offset in the commit command
      final var offsets = command.getOffsets();
      assertEquals(100L, offsets.get(PARTITION).offset(), "Should commit exactly the processed offset (99+1)");
    }

    @Test
    void shouldHandleProcessingOfUnknownOffset() {
      // Act - process offset that wasn't tracked
      final var record = new ConsumerRecord<>(TOPIC, 0, 999L, "key", "value");
      offsetManager.markOffsetProcessed(record);

      // Assert - should not throw exceptions or cause issues
      assertDoesNotThrow(() -> offsetManager.commitSyncAndWait(1));
    }

    @Test
    void shouldIgnoreDuplicateOffsetTracking() throws Exception {
      // Track the same offset multiple times
      final var record = new ConsumerRecord<>(TOPIC, 0, 101L, "key", "value");
      offsetManager.trackOffset(record);
      offsetManager.trackOffset(record); // Duplicate
      offsetManager.markOffsetProcessed(record);

      // Act - initiate commit
      final var commitFuture = CompletableFuture.supplyAsync(() -> {
        try {
          return offsetManager.commitSyncAndWait(1);
        } catch (final InterruptedException e) {
          return false;
        }
      });

      // Wait for the command to be generated
      Thread.sleep(100);

      // Get and verify command
      final var command = commandQueue.poll(1000, TimeUnit.MILLISECONDS);
      assertNotNull(command, "Should have generated a commit command");
      assertEquals(ConsumerCommand.COMMIT_OFFSETS, command, "Command should be of type COMMIT_OFFSETS");

      // Verify the offsets in the command
      var offsets = command.getOffsets();
      assertNotNull(offsets, "Command should contain offsets");
      assertTrue(offsets.containsKey(PARTITION), "Command should contain our partition");
      assertEquals(102L, offsets.get(PARTITION).offset(), "Should deduplicate tracked offsets");

      // Complete the commit
      offsetManager.notifyCommitComplete(command.getCommitId(), true);

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
      final var record0 = new ConsumerRecord<>(TOPIC, 0, 101L, "key", "value");
      final var record1 = new ConsumerRecord<>(TOPIC, 1, 201L, "key", "value");
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
      offsetManager.notifyCommitComplete(command.getCommitId(), true);

      // Assert - check the command's offsets
      final var committedOffsets = command.getOffsets();
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
      final var concurrentManager = OffsetManager
        .builder(mockConsumer)
        .withCommandQueue(commandQueue)
        .withCommitInterval(Duration.ofMillis(500))
        .build();

      final var threadCount = 5;
      final var offsetsPerThread = 100;
      final var startLatch = new CountDownLatch(1);
      final var completionLatch = new CountDownLatch(threadCount);

      // Create multiple threads updating offsets
      for (var i = 0; i < threadCount; i++) {
        final int threadId = i;
        new Thread(() -> {
          try {
            startLatch.await(); // Wait for all threads to be ready
            // Each thread processes its own range of offsets
            for (int j = 0; j < offsetsPerThread; j++) {
              final var offset = threadId * offsetsPerThread + j;
              final var record = new ConsumerRecord<>(TOPIC, 0, offset, "key", "value");
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

      // Give time for offsets to be processed
      Thread.sleep(200);

      // Force final commit and verify
      concurrentManager.commitSyncAndWait(1);
    }
  }

  @Nested
  class ConfigurationTests {

    @Test
    void shouldRespectBuilderConfigurations() {
      // Create a manager with custom configuration
      final var record = new ConsumerRecord<>(TOPIC, 0, 101L, "key", "value");
      final var customManager = OffsetManager
        .builder(mockConsumer)
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

  private ConsumerCommand performCommitAndCaptureCommand() throws Exception {
    final var commandLatch = new CountDownLatch(1);
    final var commandRef = new AtomicReference<ConsumerCommand>();

    // Start a command listener thread
    Thread
      .ofPlatform()
      .daemon()
      .start(() -> {
        try {
          final var cmd = commandQueue.poll(30, TimeUnit.SECONDS);
          if (cmd != null) {
            commandRef.set(cmd);
            commandLatch.countDown();
          }
        } catch (final InterruptedException e) {
          // Ignore interruption
        }
      });

    // Start the commit operation
    CompletableFuture.runAsync(() -> {
      try {
        offsetManager.commitSyncAndWait(30);
      } catch (final InterruptedException e) {
        // Ignore interruption
      }
    });

    // Wait for command
    final var commandReceived = commandLatch.await(10, TimeUnit.SECONDS);
    assertTrue(commandReceived, "Command should be produced within timeout");

    final var command = commandRef.get();
    assertNotNull(command, "Commit command should be generated");

    return command;
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
}
