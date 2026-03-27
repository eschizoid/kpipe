package org.kpipe.consumer;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.kpipe.consumer.enums.OffsetState;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class RebalanceListenerTest {

  @Mock
  private Consumer<String, String> mockConsumer;

  @Mock
  private Queue<ConsumerCommand> commandQueue;

  @Mock
  private ConsumerCommand consumerCommand;

  @Captor
  private ArgumentCaptor<Map<TopicPartition, OffsetAndMetadata>> offsetCaptor;

  private RebalanceListener listener;
  private AtomicReference<OffsetState> state;
  private Map<TopicPartition, ConcurrentSkipListSet<Long>> pendingOffsets;
  private Map<TopicPartition, Long> highestProcessedOffsets;

  private final TopicPartition partition0 = new TopicPartition("test", 0);
  private final TopicPartition partition1 = new TopicPartition("test", 1);
  private final TopicPartition partition2 = new TopicPartition("test", 2);

  @BeforeEach
  void setUp() {
    state = new AtomicReference<>(OffsetState.RUNNING);
    pendingOffsets = new ConcurrentHashMap<>();
    highestProcessedOffsets = new ConcurrentHashMap<>();
    listener = new RebalanceListener(state, pendingOffsets, highestProcessedOffsets, mockConsumer, commandQueue);
  }

  @Test
  void shouldCommitOffsetsWhenPartitionsRevoked() {
    // Arrange
    final var pendingSet = new ConcurrentSkipListSet<Long>();
    pendingSet.add(105L);
    pendingOffsets.put(partition0, pendingSet);

    // Set up highest processed offsets
    highestProcessedOffsets.put(partition1, 200L);

    // Act
    listener.onPartitionsRevoked(List.of(partition0, partition1));

    // Assert
    verify(mockConsumer).commitSync(offsetCaptor.capture());

    // Verify the offsets in the captured map
    final var committedOffsets = offsetCaptor.getValue();
    assertEquals(105L, committedOffsets.get(partition0).offset());
    assertEquals(201L, committedOffsets.get(partition1).offset());

    // Also verify maps are cleaned up
    assertFalse(pendingOffsets.containsKey(partition0));
    assertFalse(pendingOffsets.containsKey(partition1));
    assertFalse(highestProcessedOffsets.containsKey(partition0));
    assertFalse(highestProcessedOffsets.containsKey(partition1));
  }

  @Test
  void shouldHandlePartialRevocation() {
    // Arrange
    pendingOffsets.put(partition0, new ConcurrentSkipListSet<>());
    pendingOffsets.put(partition2, new ConcurrentSkipListSet<>());

    highestProcessedOffsets.put(partition0, 110L);
    highestProcessedOffsets.put(partition2, 310L);

    // Act - revoke only partition0
    listener.onPartitionsRevoked(List.of(partition0));

    // Assert
    // Revoked partition should be removed
    assertFalse(pendingOffsets.containsKey(partition0));
    assertFalse(highestProcessedOffsets.containsKey(partition0));

    // Non-revoked partition should remain
    assertTrue(pendingOffsets.containsKey(partition2));
    assertTrue(highestProcessedOffsets.containsKey(partition2));
  }

  @Test
  void shouldNotCommitWhenNoOffsetsToCommit() {
    // Arrange - empty maps
    final var partitions = List.of(partition0, partition1);

    // Act
    assertDoesNotThrow(() -> listener.onPartitionsRevoked(partitions));

    // Assert - maps should still be empty
    assertTrue(pendingOffsets.isEmpty(), "pendingOffsets should remain empty");
    assertTrue(highestProcessedOffsets.isEmpty(), "highestProcessedOffsets should remain empty");
  }

  @Test
  void shouldClearStateOnPartitionsAssigned() {
    // Arrange - setup initial state
    pendingOffsets.put(partition0, new ConcurrentSkipListSet<>());
    highestProcessedOffsets.put(partition0, 104L);

    // Act
    listener.onPartitionsAssigned(List.of(partition0));

    // Assert
    assertFalse(pendingOffsets.containsKey(partition0));
    assertFalse(highestProcessedOffsets.containsKey(partition0));
  }

  @Test
  void shouldDoNothingWhenStopped() {
    // Arrange
    state.set(OffsetState.STOPPED);
    pendingOffsets.put(partition0, new ConcurrentSkipListSet<>());

    // Act
    listener.onPartitionsRevoked(List.of(partition0));
    listener.onPartitionsAssigned(List.of(partition0));

    // Assert
    assertTrue(pendingOffsets.containsKey(partition0));
  }

  @Test
  void shouldHandleCommitException() {
    // Arrange
    pendingOffsets.put(partition0, new ConcurrentSkipListSet<>());

    // Act & Assert
    assertDoesNotThrow(() -> listener.onPartitionsRevoked(List.of(partition0)));

    // State should still be cleared despite the exception
    assertFalse(pendingOffsets.containsKey(partition0));
  }

  @Test
  void shouldDrainCommandQueueForRevokedPartitions() {
    // Arrange - a CommitOffset command with offsets for partition0 and partition1
    final var offsets = Map.of(partition0, new OffsetAndMetadata(100L), partition1, new OffsetAndMetadata(200L));
    commandQueue.offer(new ConsumerCommand.CommitOffsets(offsets, "commit-1"));

    // Act - revoke partition0 only
    listener.onPartitionsRevoked(List.of(partition0));

    // Assert - the remaining command should only have partition1
    final var remaining = new ArrayList<>(commandQueue);
    assertEquals(1, remaining.size());
    final var commitCmd = assertInstanceOf(ConsumerCommand.CommitOffsets.class, remaining.getFirst());
    assertFalse(commitCmd.offsets().containsKey(partition0), "partition0 should be filtered out");
    assertTrue(commitCmd.offsets().containsKey(partition0), "partition1 should be filtered out");
    assertEquals(200L, commitCmd.offsets().get(partition1).offset());
  }

  @Test
  void shouldPreserveCommandsWithoutOffsets() {
    // Arrange - a non-CommitOffsets command
    commandQueue.offer(new ConsumerCommand.Pause());

    // Act
    listener.onPartitionsRevoked(List.of(partition0));

    // Assert - Pause command should still be in the queue
    final var remaining = new ArrayList<>(commandQueue);
    assertEquals(1, remaining.size());
    assertInstanceOf(ConsumerCommand.Pause.class, remaining.getFirst());
  }

  @Test
  void shouldCompletelyRemoveCommandsForAllRevokedPartitions() {
    // Arrange - a CommitOffsets command with offsets only for partition0
    final var offsets = Map.of(partition0, new OffsetAndMetadata(100L));
    commandQueue.offer(new ConsumerCommand.CommitOffsets(offsets, "commit-1"));

    // Act - revoke partition0
    listener.onPartitionsRevoked(List.of(partition0));

    // Assert - the command should be completely removed (no remaining partitions)
    final var remaining = new ArrayList<>(commandQueue);
    assertTrue(remaining.isEmpty(), "Command with only revoked partitions should be removed entirely");
  }

  @Test
  void shouldDoNothingWhenCommandQueueIsEmpty() {
    // Arrange  - empty command queue

    // Act
    listener.onPartitionsRevoked(List.of(partition0));

    // Assert - queue should still be empty
    assertTrue(commandQueue.isEmpty());
  }

  @Test
  void shouldSkipDrainingCommandQueueWhenStopped() {
    // Arrange
    state.set(OffsetState.STOPPED);
    commandQueue.offer(new ConsumerCommand.Pause());

    // Act
    listener.onPartitionsRevoked(List.of(partition0));

    // Assert - command should still be in the queue (not drain)
    assertEquals(1, commandQueue.size());
  }
}
