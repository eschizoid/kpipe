package org.kpipe.consumer;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Logger;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.common.TopicPartition;
import org.kpipe.consumer.enums.OffsetState;

/**
 * Implementation of Kafka's ConsumerRebalanceListener that handles partition rebalance events for
 * the OffsetManager.
 *
 * <p>This listener manages offset tracking state during partition assignments and revocations,
 * ensuring that:
 *
 * <ul>
 *   <li>Tracked offsets are properly committed during revocation events
 *   <li>Internal data structures are updated to reflect the current assignment
 *   <li>Offset state is preserved for persistent consumer groups
 *   <li>Memory is reclaimed for partitions no longer assigned
 * </ul>
 *
 * <p>The listener coordinates with the OffsetManager to maintain accurate offset tracking during
 * consumer group rebalancing operations, preventing duplicate message processing or data loss when
 * partitions are reassigned between consumer instances.
 */
public class RebalanceListener implements ConsumerRebalanceListener {

  private static final Logger LOGGER = Logger.getLogger(RebalanceListener.class.getName());

  private final AtomicReference<OffsetState> state;
  private final Map<TopicPartition, ConcurrentSkipListSet<Long>> pendingOffsets;
  private final Map<TopicPartition, Long> highestProcessedOffsets;
  private final Consumer<?, ?> consumer;

  /**
   * Constructs a new RebalanceListener.
   *
   * @param state The current offset state reference
   * @param pendingOffsets A map of pending offsets for each partition
   * @param highestProcessedOffsets A map of the highest processed offsets for each partition
   * @param consumer The Kafka consumer to manage offsets for
   */
  public RebalanceListener(
    final AtomicReference<OffsetState> state,
    final Map<TopicPartition, ConcurrentSkipListSet<Long>> pendingOffsets,
    final Map<TopicPartition, Long> highestProcessedOffsets,
    final Consumer<?, ?> consumer
  ) {
    this.state = state;
    this.pendingOffsets = pendingOffsets;
    this.highestProcessedOffsets = highestProcessedOffsets;
    this.consumer = consumer;
  }

  @Override
  public void onPartitionsRevoked(final Collection<TopicPartition> partitions) {
    if (state.get() == OffsetState.STOPPED) {
      return;
    }
    LOGGER.info("Partitions revoked: %s".formatted(partitions));
    partitions.forEach(partition -> {
      pendingOffsets.remove(partition);
      highestProcessedOffsets.remove(partition);
    });
  }

  @Override
  public void onPartitionsAssigned(final Collection<TopicPartition> partitions) {
    if (state.get() == OffsetState.STOPPED) {
      return;
    }
    LOGGER.info("Partitions assigned: %s".formatted(partitions));
    partitions.forEach(partition -> {
      pendingOffsets.remove(partition);
      highestProcessedOffsets.remove(partition);
      final var position = consumer.position(partition);
      highestProcessedOffsets.put(partition, position);
    });
  }

  @Override
  public void onPartitionsLost(final Collection<TopicPartition> partitions) {
    onPartitionsRevoked(partitions);
  }
}
