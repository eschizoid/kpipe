package org.kpipe.consumer;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.kpipe.consumer.enums.OffsetState;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class RebalanceListenerTest {

  @Mock
  private Consumer<String, String> mockConsumer;

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
    listener = new RebalanceListener(state, pendingOffsets, highestProcessedOffsets, mockConsumer);
  }

  @Test
  void shouldCommitOffsetsWhenPartitionsRevoked() {
    // Act
    listener.onPartitionsRevoked(List.of(partition0, partition1));

    // Assert
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

    // Mock the consumer position
    when(mockConsumer.position(partition0)).thenReturn(200L);

    // Act
    listener.onPartitionsAssigned(List.of(partition0));

    // Assert
    assertFalse(pendingOffsets.containsKey(partition0));
    assertTrue(highestProcessedOffsets.containsKey(partition0));
    assertEquals(200L, highestProcessedOffsets.get(partition0));
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
}
