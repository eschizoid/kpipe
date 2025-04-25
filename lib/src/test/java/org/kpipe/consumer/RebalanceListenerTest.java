package org.kpipe.consumer;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.util.List;
import java.util.Map;
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

  @Captor
  private ArgumentCaptor<Map<TopicPartition, OffsetAndMetadata>> offsetCaptor;

  private AtomicReference<OffsetState> state;
  private Map<TopicPartition, ConcurrentSkipListSet<Long>> pendingOffsets;
  private Map<TopicPartition, Long> nextOffsetsToCommit;
  private Map<TopicPartition, Long> highestProcessedOffsets;
  private RebalanceListener listener;

  private final TopicPartition partition0 = new TopicPartition("test-topic", 0);
  private final TopicPartition partition1 = new TopicPartition("test-topic", 1);
  private final TopicPartition partition2 = new TopicPartition("test-topic", 2);

  @BeforeEach
  void setUp() {
    state = new AtomicReference<>(OffsetState.RUNNING);
    pendingOffsets = new ConcurrentHashMap<>();
    nextOffsetsToCommit = new ConcurrentHashMap<>();
    highestProcessedOffsets = new ConcurrentHashMap<>();

    listener = new RebalanceListener(state, pendingOffsets, nextOffsetsToCommit, highestProcessedOffsets, mockConsumer);
  }

  @Test
  void shouldCommitOffsetsWhenPartitionsRevoked() {
    // Arrange
    nextOffsetsToCommit.put(partition0, 100L);
    nextOffsetsToCommit.put(partition1, 200L);

    pendingOffsets.put(partition0, new ConcurrentSkipListSet<>());
    pendingOffsets.get(partition0).add(105L);

    highestProcessedOffsets.put(partition0, 104L);
    highestProcessedOffsets.put(partition1, 205L);

    // Act
    listener.onPartitionsRevoked(List.of(partition0, partition1));

    // Assert
    verify(mockConsumer).commitSync(offsetCaptor.capture());

    final var committedOffsets = offsetCaptor.getValue();
    assertEquals(2, committedOffsets.size());
    assertEquals(100L, committedOffsets.get(partition0).offset());
    assertEquals(200L, committedOffsets.get(partition1).offset());

    // State should be cleared
    assertFalse(pendingOffsets.containsKey(partition0));
    assertFalse(nextOffsetsToCommit.containsKey(partition0));
    assertFalse(highestProcessedOffsets.containsKey(partition0));
  }

  @Test
  void shouldHandlePartialRevocation() {
    // Arrange - setup state for multiple partitions
    nextOffsetsToCommit.put(partition0, 100L);
    nextOffsetsToCommit.put(partition1, 200L);
    nextOffsetsToCommit.put(partition2, 300L);

    pendingOffsets.put(partition0, new ConcurrentSkipListSet<>());
    pendingOffsets.put(partition2, new ConcurrentSkipListSet<>());

    highestProcessedOffsets.put(partition0, 104L);
    highestProcessedOffsets.put(partition1, 205L);
    highestProcessedOffsets.put(partition2, 310L);

    // Act - only revoke some partitions
    listener.onPartitionsRevoked(List.of(partition0, partition1));

    // Assert
    verify(mockConsumer).commitSync(offsetCaptor.capture());

    final var committedOffsets = offsetCaptor.getValue();
    assertEquals(2, committedOffsets.size());
    assertEquals(100L, committedOffsets.get(partition0).offset());
    assertEquals(200L, committedOffsets.get(partition1).offset());

    // Revoked partitions should be cleared
    assertFalse(pendingOffsets.containsKey(partition0));
    assertFalse(nextOffsetsToCommit.containsKey(partition0));
    assertFalse(highestProcessedOffsets.containsKey(partition0));

    // Non-revoked partition should remain
    assertTrue(pendingOffsets.containsKey(partition2));
    assertTrue(nextOffsetsToCommit.containsKey(partition2));
    assertTrue(highestProcessedOffsets.containsKey(partition2));
  }

  @Test
  void shouldNotCommitWhenNoOffsetsToCommit() {
    // Arrange - empty maps
    final var partitions = List.of(partition0, partition1);

    // Act
    listener.onPartitionsRevoked(partitions);

    // Assert
    verify(mockConsumer, never()).commitSync(anyMap());
  }

  @Test
  void shouldClearStateOnPartitionsAssigned() {
    // Arrange - setup initial state
    pendingOffsets.put(partition0, new ConcurrentSkipListSet<>());
    nextOffsetsToCommit.put(partition0, 100L);
    highestProcessedOffsets.put(partition0, 104L);

    // Act
    listener.onPartitionsAssigned(List.of(partition0));

    // Assert
    assertFalse(pendingOffsets.containsKey(partition0));
    assertFalse(nextOffsetsToCommit.containsKey(partition0));

    // Note: highestProcessedOffsets is not cleared on assignment
    assertTrue(highestProcessedOffsets.containsKey(partition0));
  }

  @Test
  void shouldDoNothingWhenStopped() {
    // Arrange
    state.set(OffsetState.STOPPED);
    nextOffsetsToCommit.put(partition0, 100L);

    // Act
    listener.onPartitionsRevoked(List.of(partition0));
    listener.onPartitionsAssigned(List.of(partition0));

    // Assert
    verify(mockConsumer, never()).commitSync(anyMap());
    assertTrue(nextOffsetsToCommit.containsKey(partition0));
  }

  @Test
  void shouldHandleCommitException() {
    // Arrange
    nextOffsetsToCommit.put(partition0, 100L);
    doThrow(new RuntimeException("Commit failed")).when(mockConsumer).commitSync(anyMap());

    // Act & Assert
    assertDoesNotThrow(() -> listener.onPartitionsRevoked(List.of(partition0)));

    // State should still be cleared despite the exception
    assertFalse(nextOffsetsToCommit.containsKey(partition0));
  }
}
