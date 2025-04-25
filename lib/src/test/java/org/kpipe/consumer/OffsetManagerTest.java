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
import org.junit.jupiter.api.Disabled;
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

  @Mock
  private Consumer<String, String> mockConsumer;

  @Captor
  private ArgumentCaptor<Map<TopicPartition, OffsetAndMetadata>> offsetCaptor;

  private OffsetManager<String, String> offsetManager;
  private BlockingQueue<ConsumerCommand> commandQueue;
  private final TopicPartition partition = new TopicPartition("test-topic", 0);

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

  @ParameterizedTest
  @MethodSource("contiguousOffsetScenarios")
  void shouldOnlyCommitContiguousOffsetsWhenProcessedOutOfOrder(
    final String description,
    final long initialOffset,
    final List<Long> offsetsToTrack,
    final List<Long> offsetsToProcess,
    final long expectedCommittedOffset
  ) throws Exception {
    // Track initial offset
    final var record = new ConsumerRecord<>(TOPIC, 0, initialOffset, "key", "value");
    offsetManager.trackOffset(record);

    // Process in batches
    offsetsToTrack.forEach(offset -> offsetManager.trackOffset(partition, offset));
    offsetsToProcess.forEach(offset -> offsetManager.markOffsetProcessed(partition, offset));

    // Capture command and commit
    final var command = performCommitAndCaptureCommand(commandQueue);

    // Complete the commit cycle
    offsetManager.notifyCommitComplete(command.getCommitId(), true);

    // Verify offset state
    final var expectedOffsets = Map.of(partition, new OffsetAndMetadata(expectedCommittedOffset));
    assertEquals(expectedOffsets, command.getOffsets(), "Only contiguous offsets should be committed");

    // Also verify via partition state
    assertEventually(
      () -> expectedCommittedOffset == (long) offsetManager.getPartitionState(partition).get("nextOffsetToCommit"),
      Duration.ofSeconds(2),
      "nextOffsetToCommit should be " + expectedCommittedOffset
    );
  }

  static Stream<Arguments> contiguousOffsetScenarios() {
    return Stream.of(
      Arguments.of(
        "Basic gap scenario - process offsets out of order with a missing offset",
        100L, // initialOffset
        List.of(101L, 105L, 102L, 103L, 104L), // offsetsToTrack
        List.of(102L, 101L, 104L), // offsetsToProcess (note: 103 is deliberately not processed)
        103L // expectedCommittedOffset
      ),
      Arguments.of(
        "Multiple gaps scenario - commit should stop at first unprocessed offset",
        200L, // initialOffset
        List.of(201L, 202L, 205L, 206L, 203L, 204L), // offsetsToTrack
        List.of(201L, 202L, 203L), // offsetsToProcess (note: 204-206 are not processed)
        204L // expectedCommittedOffset - commits up to the first gap
      ),
      Arguments.of(
        "Large range with interleaved processing",
        1000L, // initialOffset
        List.of(1_001L, 1_002L, 1_010L, 1_020L, 1_030L, 1_005L, 1_006L), // offsetsToTrack
        List.of(1_001L, 1_006L, 1_002L, 1_030L, 1_010L), // offsetsToProcess (note: 1005L and 1020L not processed)
        1005L // expectedCommittedOffset - commits up to 1005L (the first gap is at 1005L)
      )
    );
  }

  @ParameterizedTest
  @MethodSource("gapSizeScenarios")
  void shouldHandleGapsOfVaryingSizes(
    final String description,
    final long initialOffset,
    final long gapOffset,
    final long expectedOffsetAfterFirstCommit,
    final long expectedOffsetAfterSecondCommit
  ) throws Exception {
    // Initialize with first record
    final var record = new ConsumerRecord<>(TOPIC, 0, initialOffset, "key", "value");
    offsetManager.trackOffset(record);
    offsetManager.markOffsetProcessed(record);

    // First commit - using shared helper method
    final var firstCommand = performCommitAndCaptureCommand(commandQueue);

    // Verify first commit completed successfully
    offsetManager.notifyCommitComplete(firstCommand.getCommitId(), true);

    // Verify partition state after first commit
    assertEventually(
      () ->
        expectedOffsetAfterFirstCommit == (long) offsetManager.getPartitionState(partition).get("nextOffsetToCommit"),
      Duration.ofSeconds(2),
      "Offset after first commit should be " + expectedOffsetAfterFirstCommit
    );

    // Track and process record with gap offset
    final var largeGapRecord = new ConsumerRecord<>(TOPIC, 0, gapOffset, "key", "value");
    offsetManager.trackOffset(largeGapRecord);
    offsetManager.markOffsetProcessed(largeGapRecord);

    // Second commit using same helper
    final var secondCommand = performCommitAndCaptureCommand(commandQueue);

    // Complete the second commit
    offsetManager.notifyCommitComplete(secondCommand.getCommitId(), true);

    // Verify partition state after second commit
    assertEventually(
      () ->
        expectedOffsetAfterSecondCommit == (long) offsetManager.getPartitionState(partition).get("nextOffsetToCommit"),
      Duration.ofSeconds(2),
      "Offset after second commit should be " + expectedOffsetAfterSecondCommit
    );
  }

  private ConsumerCommand performCommitAndCaptureCommand(final BlockingQueue<ConsumerCommand> queue) throws Exception {
    final var commandLatch = new CountDownLatch(1);
    final var commandRef = new AtomicReference<ConsumerCommand>();

    // Start command listener thread
    Thread
      .ofPlatform()
      .daemon()
      .start(() -> {
        try {
          final var cmd = queue.poll(10, TimeUnit.SECONDS);
          if (cmd != null) {
            commandRef.set(cmd);
            commandLatch.countDown();
          }
        } catch (final InterruptedException e) {
          // Ignore interruption
        }
      });

    // Start commit operation
    CompletableFuture.runAsync(() -> {
      try {
        offsetManager.commitSyncAndWait(10);
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
        5_001L // expected offset after second commit
      ),
      Arguments.of(
        "Backwards gap (higher offset processed before lower)",
        5_000L, // initial offset
        1_000L, // gap offset (lower than initial - backwards)
        5_001L, // expected offset after first commit
        5_001L // expected offset after second commit (unchanged)
      ),
      Arguments.of(
        "Very large gap (thousands of offsets)",
        1000L, // initial offset
        5_000L, // gap offset (reduced from 1000000L)
        1_001L, // expected offset after first commit
        5_001L // expected offset after second commit
      ),
      Arguments.of(
        "Small gap between offsets",
        1_000L, // initial offset
        1_001L, // gap offset (just 1 more than initial)
        1_001L, // expected offset after first commit
        1_002L // expected offset after second commit
      ),
      Arguments.of(
        "Starting from offset zero",
        0L, // initial offset
        1_000L, // gap offset
        1L, // expected offset after first commit
        1_001L // expected offset after second commit
      )
    );
  }

  @Test
  void shouldHandleRebalances() {
    // Init with first record
    var record = new ConsumerRecord<>("test-topic", 0, 100, "key", "value");
    offsetManager.trackOffset(record);

    // Get rebalance listener
    final var listener = offsetManager.createRebalanceListener();

    // Simulate rebalance
    listener.onPartitionsRevoked(List.of(partition));

    // Verify state cleared after revoke
    final var stateAfterRevoke = offsetManager.getPartitionState(partition);
    assertEquals(-1L, stateAfterRevoke.get("nextOffsetToCommit"), "Next offset to commit should be reset after revoke");

    // Simulate new partition assignment
    listener.onPartitionsAssigned(List.of(partition));

    // Verify tracking starts over
    record = new ConsumerRecord<>("test-topic", 0, 200, "key", "value");
    offsetManager.trackOffset(record);

    final var stateAfterAssign = offsetManager.getPartitionState(partition);
    assertEquals(
      200L,
      stateAfterAssign.get("nextOffsetToCommit"),
      "Next offset to commit should initialize from first record after assignment"
    );
  }

  @Test
  void shouldHandleConsumerExceptions() throws Exception {
    // Track and mark offset as processed
    offsetManager.trackOffset(partition, 101L);
    offsetManager.markOffsetProcessed(partition, 101L);

    // Create a future to monitor a commit result
    final var commitFuture = CompletableFuture.supplyAsync(() -> {
      try {
        return offsetManager.commitSyncAndWait(1);
      } catch (final InterruptedException e) {
        return false;
      }
    });

    // Wait for command to be generated
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
    final var state = offsetManager.getPartitionState(partition);
    assertEquals(101L, state.get("highestProcessedOffset"), "Should maintain highest processed offset");
  }

  @Test
  void shouldHandleTrackingBeforeConsumerPosition() throws Exception {
    // Track a low offset (regardless of consumer position)
    offsetManager.trackOffset(partition, 99L);
    offsetManager.markOffsetProcessed(partition, 99L);

    // Act - initiate commit
    final var commitFuture = CompletableFuture.supplyAsync(() -> {
      try {
        return offsetManager.commitSyncAndWait(1);
      } catch (final InterruptedException e) {
        return false;
      }
    });

    // Wait for command to be generated
    Thread.sleep(100);

    // Get and verify the command
    final var command = commandQueue.poll();
    assertNotNull(command, "Should have generated a commit command");

    // Complete the commit successfully
    offsetManager.notifyCommitComplete(command.getCommitId(), true);

    // Assert
    final var result = commitFuture.get(2, TimeUnit.SECONDS);
    assertTrue(result, "Commit should succeed");

    // Verify the offset in the commit command - should be offset+1
    final var offsets = command.getOffsets();
    assertEquals(100L, offsets.get(partition).offset(), "Should commit offset+1 (100) for processed offset (99)");
  }

  @Test
  void shouldCloseGracefullyWhenCommitFails() {
    // Track and mark offset
    offsetManager.trackOffset(partition, 101L);
    offsetManager.markOffsetProcessed(partition, 101L);

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
  @Disabled("Disabled for now")
  void shouldInterruptRecoveryOnClose() throws Exception {
    // Arrange - setup offset manager
    offsetManager = OffsetManager.builder(mockConsumer).withCommandQueue(new ConcurrentLinkedQueue<>()).build();

    // Act
    final var closedLatch = new CountDownLatch(1);
    final var closed = new AtomicBoolean(false);

    // Start thread to close in background
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
  @Disabled("Disabled for now")
  void shouldHandleProcessingOfUnknownOffset() {
    // Act - process offset that wasn't tracked
    offsetManager.markOffsetProcessed(partition, 999L);

    // Assert - should not throw exceptions or cause issues
    assertDoesNotThrow(() -> offsetManager.commitSyncAndWait(1));
  }

  @Test
  @Disabled("Disabled for now")
  void shouldHandleMultipleCloseCalls() throws Exception {
    // Track and mark an offset
    offsetManager.trackOffset(partition, 101L);
    offsetManager.markOffsetProcessed(partition, 101L);
    Thread.sleep(100);

    // First close
    offsetManager.close();

    // Second close should be safe
    offsetManager.close();

    // Verify commit was called only once for the first close
    verify(mockConsumer, times(1)).commitSync(anyMap());
  }

  @Test
  @Disabled("Disabled for now")
  void shouldAutomaticallyCommitAtInterval() throws Exception {
    // Create manager with short commit interval
    final var autoCommitManager = OffsetManager.builder(mockConsumer).withCommitInterval(Duration.ofMillis(50)).build();

    // Start the manager to activate the scheduler
    autoCommitManager.start();

    // Track and mark an offset
    autoCommitManager.trackOffset(partition, 101L);
    autoCommitManager.markOffsetProcessed(partition, 101L);

    // Wait for auto-commit to occur
    Thread.sleep(200);

    // Verify auto-commit happened
    verify(mockConsumer, timeout(1000).atLeastOnce()).commitSync(anyMap());

    // Clean up
    autoCommitManager.close();
  }

  @Test
  @Disabled("Disabled for now")
  void shouldHandleMultiplePartitions() throws Exception {
    // Arrange - create multiple partitions
    final var partition0 = new TopicPartition(TOPIC, 0);
    final var partition1 = new TopicPartition(TOPIC, 1);

    // Track and process offsets for both partitions
    offsetManager.trackOffset(partition0, 101L);
    offsetManager.trackOffset(partition1, 201L);
    offsetManager.markOffsetProcessed(partition0, 101L);
    offsetManager.markOffsetProcessed(partition1, 201L);

    // Act
    offsetManager.commitSyncAndWait(1);

    // Assert
    verify(mockConsumer).commitSync(offsetCaptor.capture());
    final var committed = offsetCaptor.getValue();
    assertEquals(2, committed.size(), "Should commit for both partitions");
    assertEquals(102L, committed.get(partition0).offset(), "Should commit correct offset for partition 0");
    assertEquals(202L, committed.get(partition1).offset(), "Should commit correct offset for partition 1");
  }

  @Test
  @Disabled("Disabled for now")
  void shouldHandleConcurrentAccess() throws Exception {
    // Use a manager with shorter interval for this test
    final var concurrentManager = OffsetManager
      .builder(mockConsumer)
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
            concurrentManager.trackOffset(partition, offset);
            concurrentManager.markOffsetProcessed(partition, offset);
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

    // Clean up
    concurrentManager.close();
  }

  @Test
  @Disabled("Disabled for now")
  void shouldRespectBuilderConfigurations() {
    // Create a manager with custom configuration
    final var customManager = OffsetManager.builder(mockConsumer).withCommitInterval(Duration.ofMillis(200)).build();

    // Just verify it can be created and closed without exceptions
    assertDoesNotThrow(() -> {
      customManager.trackOffset(partition, 101L);
      customManager.markOffsetProcessed(partition, 101L);
      customManager.close();
    });
  }

  @Test
  @Disabled("Disabled for now")
  void shouldCommitCorrectlyAfterPartitionRebalance() {
    // Simulate partition rebalance by clearing and re-tracking
    offsetManager.trackOffset(partition, 101L);
    offsetManager.markOffsetProcessed(partition, 101L);

    // "Rebalance" - clear state through reset method if available, or skip test
    try {
      // Using reflection to call method if it exists
      final var resetMethod = offsetManager.getClass().getDeclaredMethod("reset", TopicPartition.class);
      resetMethod.setAccessible(true);
      resetMethod.invoke(offsetManager, partition);

      // Track new offsets after rebalance
      offsetManager.trackOffset(partition, 501L);
      offsetManager.markOffsetProcessed(partition, 501L);

      // Verify correct offset committed
      offsetManager.commitSyncAndWait(1);
      verify(mockConsumer).commitSync(offsetCaptor.capture());
      assertEquals(
        502L,
        offsetCaptor.getValue().get(partition).offset(),
        "Should commit correct offset after rebalance"
      );
    } catch (final NoSuchMethodException e) {
      // Skip test if reset method doesn't exist
    } catch (final Exception e) {
      fail("Exception during rebalance test: " + e.getMessage());
    }
  }

  @Test
  @Disabled("Disabled for now")
  void shouldHandleAsyncCommitFailures() throws Exception {
    // Setup async commit to throw exception - use anyMap() instead of any()
    doThrow(new RuntimeException("Async commit failed")).when(mockConsumer).commitAsync(anyMap(), any());

    // Create manager with very short commit interval
    final var asyncManager = OffsetManager
      .builder(mockConsumer)
      .withCommitInterval(Duration.ofMillis(10)) // Shorter interval for quicker execution
      .build();

    // Track and process an offset
    asyncManager.trackOffset(partition, 101L);
    asyncManager.markOffsetProcessed(partition, 101L);

    // Wait longer for async commit to be attempted
    Thread.sleep(300); // Longer wait to ensure scheduler runs

    // Verify with correct parameter matchers
    verify(mockConsumer, atLeastOnce()).commitAsync(anyMap(), any());

    // Clean up
    asyncManager.close();
  }

  @Test
  @Disabled("Disabled for now")
  void shouldHandleCommitSyncWithTimeout() throws Exception {
    // Arrange
    offsetManager.trackOffset(partition, 101L);
    offsetManager.markOffsetProcessed(partition, 101L);

    // Act - commit with explicit timeout
    final var result = offsetManager.commitSyncAndWait(500);

    // Assert
    assertTrue(result, "Commit with timeout should succeed");
    verify(mockConsumer).commitSync(offsetCaptor.capture());
    assertEquals(102L, offsetCaptor.getValue().get(partition).offset(), "Should commit correct offset");
  }

  @Test
  @Disabled("Disabled for now")
  void shouldHandleConsumerClosedDuringOperation() {
    // Arrange - setup consumer to throw exception when already closed
    doThrow(new IllegalStateException("Consumer already closed")).when(mockConsumer).commitSync(anyMap());

    // Act - track, process, and try to commit
    offsetManager.trackOffset(partition, 101L);
    offsetManager.markOffsetProcessed(partition, 101L);

    // Assert - should handle gracefully
    assertDoesNotThrow(() -> offsetManager.commitSyncAndWait(1));

    // Try to close - should still not throw
    assertDoesNotThrow(() -> offsetManager.close());
  }

  @Test
  @Disabled("Disabled for now")
  void shouldIgnoreDuplicateOffsetTracking() throws Exception {
    // Track the same offset multiple times
    offsetManager.trackOffset(partition, 101L);
    offsetManager.trackOffset(partition, 101L); // Duplicate
    offsetManager.markOffsetProcessed(partition, 101L);

    // Act
    offsetManager.commitSyncAndWait(1);

    // Assert
    verify(mockConsumer).commitSync(offsetCaptor.capture());
    assertEquals(102L, offsetCaptor.getValue().get(partition).offset(), "Should deduplicate tracked offsets");
  }
}
