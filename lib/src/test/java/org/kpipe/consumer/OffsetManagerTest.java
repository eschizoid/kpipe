package org.kpipe.consumer;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
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
  private final TopicPartition partition = new TopicPartition("test-topic", 0);

  @AfterEach
  void tearDown() {
    offsetManager.close();
  }

  @Test
  void shouldHandleEmptyPartitions() throws Exception {
    // Act - commit with no tracked offsets
    final var commandQueue = new ConcurrentLinkedQueue<ConsumerCommand>();
    offsetManager = OffsetManager.builder(mockConsumer).withCommandQueue(commandQueue).build();

    final var result = offsetManager.commitSyncAndWait(1);

    // Assert
    assertTrue(result, "Should handle empty partitions gracefully");
    verify(mockConsumer, never()).commitSync(anyMap());
  }

  @Test
  void shouldOnlyCommitContiguousOffsetsWhenProcessedOutOfOrder() throws Exception {
    // Arrange
    final var commandQueue = new ConcurrentLinkedQueue<ConsumerCommand>();
    final var mockConsumer = mock(Consumer.class);
    when(mockConsumer.position(partition)).thenReturn(100L);

    offsetManager = OffsetManager.builder(mockConsumer).withCommandQueue(commandQueue).build();

    offsetManager.start();

    // Track offsets (out of order)
    offsetManager.trackOffset(partition, 101L);
    offsetManager.trackOffset(partition, 105L);
    offsetManager.trackOffset(partition, 102L);
    offsetManager.trackOffset(partition, 103L);
    offsetManager.trackOffset(partition, 104L);

    // Act
    // Mark offsets as processed in non-sequential order
    // Note: 103 is deliberately not processed
    offsetManager.markOffsetProcessed(partition, 102L);
    offsetManager.markOffsetProcessed(partition, 101L);
    offsetManager.markOffsetProcessed(partition, 104L);

    // Start commit in a separate thread so we can simulate the response
    final var testFuture = new CompletableFuture<Boolean>();
    Thread
      .ofVirtual()
      .start(() -> {
        try {
          boolean result = offsetManager.commitSyncAndWait(5);
          testFuture.complete(result);
        } catch (final InterruptedException e) {
          testFuture.completeExceptionally(e);
        }
      });

    // Wait for command to be added to queue
    Thread.sleep(100);

    // Extract command and verify
    final var command = commandQueue.poll();
    assertNotNull(command, "Commit command should be generated");
    assertEquals(ConsumerCommand.COMMIT_OFFSETS, command);

    // Simulate the consumer's response
    offsetManager.notifyCommitComplete(command.getCommitId(), true);

    // Wait for commit result
    final var result = testFuture.get(5, TimeUnit.SECONDS);

    // Assert
    assertTrue(result, "Commit operation should succeed");

    // Verify only contiguous offsets (101, 102) are committed
    final var expectedOffsets = Map.of(partition, new OffsetAndMetadata(103L));
    assertEquals(expectedOffsets, command.getOffsets(), "Only contiguous offsets should be committed");

    // Verify offset manager state
    final var partitionState = offsetManager.getPartitionState(partition);
    assertEquals(103L, partitionState.get("nextOffsetToCommit"), "Next offset to commit should be 103");
  }

  @Test
  void shouldHandleVeryLargeGaps() throws InterruptedException {
    // Setup initial position
    final var commandQueue = new ConcurrentLinkedQueue<ConsumerCommand>();
    when(mockConsumer.position(partition)).thenReturn(1000L);

    // Initialize the offset manager with this partition
    offsetManager = OffsetManager.builder(mockConsumer).withCommandQueue(commandQueue).build();
    offsetManager.trackOffset(partition, 1000L);

    // Track and process offsets with large gaps
    offsetManager.trackOffset(partition, 1000L);
    offsetManager.markOffsetProcessed(partition, 1000L);

    offsetManager.trackOffset(partition, 5000L);
    offsetManager.markOffsetProcessed(partition, 5000L);

    offsetManager.trackOffset(partition, 10000L);
    offsetManager.markOffsetProcessed(partition, 10000L);

    // Create a thread to process the commit commands from the queue
    var commandProcessor = Thread
      .ofVirtual()
      .start(() -> {
        while (!Thread.currentThread().isInterrupted()) {
          final var cmd = commandQueue.poll();
          if (cmd == ConsumerCommand.COMMIT_OFFSETS) {
            // Simulate successful commit processing
            final var commitId = cmd.getCommitId();
            if (commitId != null) {
              offsetManager.notifyCommitComplete(commitId, true);
            }
          }
        }
      });

    // Give enough time for all processing to complete
    Thread.sleep(200);

    // Attempt to commit offsets
    boolean result = offsetManager.commitSyncAndWait(5);

    // Assertion that was failing
    assertTrue(result, "Commit operation should succeed");

    // Verify the offset state shows progress
    final var state = offsetManager.getPartitionState(partition);
    assertEquals(1001L, state.get("nextOffsetToCommit"));

    // Clean up the command processor
    commandProcessor.interrupt();
  }

  @Test
  @Disabled("Disabled for now")
  void shouldHandleConsumerExceptions() {
    // Arrange - position throws exception
    when(mockConsumer.position(any(TopicPartition.class))).thenThrow(new RuntimeException("Position failed"));

    // Act - this should not throw
    assertDoesNotThrow(
      () -> {
        offsetManager.trackOffset(partition, 101L);
        offsetManager.markOffsetProcessed(partition, 101L);
        Thread.sleep(200);
        offsetManager.commitSyncAndWait(1);
      },
      "Should handle consumer exceptions gracefully"
    );

    // No commits should happen due to position exception
    verify(mockConsumer, never()).commitSync(anyMap());
  }

  @Test
  @Disabled("Disabled for now")
  void shouldHandleNonContiguousTracking() throws Exception {
    // Arrange - track non-contiguous offsets
    offsetManager.trackOffset(partition, 105L);
    offsetManager.trackOffset(partition, 110L);

    // Process them
    offsetManager.markOffsetProcessed(partition, 105L);
    offsetManager.markOffsetProcessed(partition, 110L);
    Thread.sleep(200);

    // Act
    offsetManager.commitSyncAndWait(1);

    // Assert
    verify(mockConsumer).commitSync(offsetCaptor.capture());
    assertEquals(
      106L,
      offsetCaptor.getValue().get(partition).offset(),
      "Should commit after first processed offset, not considering later gaps"
    );
  }

  @Test
  @Disabled("Disabled for now")
  void shouldCloseGracefullyWhenCommitFails() {
    // Arrange - setup offset and make commit fail
    offsetManager.trackOffset(partition, 101L);
    offsetManager.markOffsetProcessed(partition, 101L);
    doThrow(new RuntimeException("Commit failed")).when(mockConsumer).commitSync(anyMap());

    // Act - close should handle failed commit
    assertDoesNotThrow(
      () -> {
        offsetManager.close();
      },
      "Should close gracefully even when final commit fails"
    );

    // Should have attempted to commit
    verify(mockConsumer).commitSync(anyMap());
  }

  @Test
  @Disabled("Disabled for now")
  void shouldHandleTrackingBeforeConsumerPosition() throws Exception {
    // Arrange - track an offset below consumer position
    offsetManager.trackOffset(partition, 99L); // Position is 100
    offsetManager.markOffsetProcessed(partition, 99L);
    Thread.sleep(200);

    // Act
    offsetManager.commitSyncAndWait(1);

    // Assert - should use consumer position as minimum
    verify(mockConsumer).commitSync(offsetCaptor.capture());
    assertNotNull(offsetCaptor.getValue().get(partition), "Should still attempt to commit even with invalid offset");
  }

  @Test
  @Disabled("Disabled for now")
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
