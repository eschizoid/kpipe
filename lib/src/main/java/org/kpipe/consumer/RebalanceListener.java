package org.kpipe.consumer;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
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
  private final Map<TopicPartition, Long> nextOffsetsToCommit;
  private final Map<TopicPartition, Long> highestProcessedOffsets;
  private final Consumer<?, ?> kafkaConsumer;

  /**
   * Constructs a new RebalanceListener.
   *
   * @param state The current offset state reference
   * @param pendingOffsets A map of pending offsets for each partition
   * @param nextOffsetsToCommit A map of next offsets to commit for each partition
   * @param highestProcessedOffsets A map of highest processed offsets for each partition
   * @param kafkaConsumer The Kafka consumer instance
   */
  public RebalanceListener(
    final AtomicReference<OffsetState> state,
    final Map<TopicPartition, ConcurrentSkipListSet<Long>> pendingOffsets,
    final Map<TopicPartition, Long> nextOffsetsToCommit,
    final Map<TopicPartition, Long> highestProcessedOffsets,
    final Consumer<?, ?> kafkaConsumer
  ) {
    this.state = state;
    this.pendingOffsets = pendingOffsets;
    this.nextOffsetsToCommit = nextOffsetsToCommit;
    this.highestProcessedOffsets = highestProcessedOffsets;
    this.kafkaConsumer = kafkaConsumer;
  }

  @Override
  public void onPartitionsRevoked(Collection<TopicPartition> revokedPartitions) {
    if (state.get() == OffsetState.STOPPED) {
      return;
    }

    final var offsetsToCommit = new HashMap<TopicPartition, OffsetAndMetadata>();

    revokedPartitions
      .stream()
      .filter(partition -> nextOffsetsToCommit.get(partition) != null)
      .forEach(partition -> {
        final var offsetToCommit = nextOffsetsToCommit.get(partition);
        offsetsToCommit.put(partition, new OffsetAndMetadata(offsetToCommit));
      });

    if (!offsetsToCommit.isEmpty()) {
      try {
        kafkaConsumer.commitSync(offsetsToCommit);
        LOGGER.info("Committed offsets for revoked partitions: %s".formatted(offsetsToCommit));
      } catch (final Exception e) {
        LOGGER.log(Level.WARNING, "Failed to commit offsets for revoked partitions", e);
      }
    }

    // Clear state for revoked partitions
    revokedPartitions.forEach(partition -> {
      pendingOffsets.remove(partition);
      nextOffsetsToCommit.remove(partition);
      highestProcessedOffsets.remove(partition);
    });
  }

  @Override
  public void onPartitionsAssigned(final Collection<TopicPartition> assignedPartitions) {
    if (state.get() == OffsetState.STOPPED) {
      return;
    }

    for (final var partition : assignedPartitions) {
      pendingOffsets.remove(partition);
      nextOffsetsToCommit.remove(partition);
    }
  }
}
