package org.kpipe.consumer;

import java.util.*;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.kpipe.consumer.enums.ConsumerCommand;
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
  private final Queue<ConsumerCommand> commandQueue;

  /**
   * Constructs a new RebalanceListener.
   *
   * @param state The current offset state reference
   * @param pendingOffsets A map of pending offsets for each partition
   * @param highestProcessedOffsets A map of the highest processed offsets for each partition
   * @param consumer The Kafka consumer to manage offsets for
   * @param commandQueue The command queue used for managing offset commit operations
   */
  public RebalanceListener(
    final AtomicReference<OffsetState> state,
    final Map<TopicPartition, ConcurrentSkipListSet<Long>> pendingOffsets,
    final Map<TopicPartition, Long> highestProcessedOffsets,
    final Consumer<?, ?> consumer,
    final Queue<ConsumerCommand> commandQueue
  ) {
    this.state = state;
    this.pendingOffsets = pendingOffsets;
    this.highestProcessedOffsets = highestProcessedOffsets;
    this.consumer = consumer;
    this.commandQueue = commandQueue;
  }

  @Override
  public void onPartitionsRevoked(final Collection<TopicPartition> partitions) {
    if (state.get() == OffsetState.STOPPED) return;
    LOGGER.info("Partitions revoked: %s".formatted(partitions));

    drainCommandQueue(partitions);

    final var offsetsToCommit = new HashMap<TopicPartition, OffsetAndMetadata>();

    partitions.forEach(partition -> {
      final var pending = pendingOffsets.get(partition);
      if (pending != null && !pending.isEmpty()) {
        offsetsToCommit.put(partition, new OffsetAndMetadata(pending.first()));
      } else {
        final var highestProcessed = highestProcessedOffsets.get(partition);
        if (highestProcessed != null) offsetsToCommit.put(partition, new OffsetAndMetadata(highestProcessed + 1));
      }
      pendingOffsets.remove(partition);
      highestProcessedOffsets.remove(partition);
    });

    if (!offsetsToCommit.isEmpty()) {
      try {
        consumer.commitSync(offsetsToCommit);
        LOGGER.info("Committed offsets for revoked partitions: %s".formatted(offsetsToCommit));
      } catch (final Exception e) {
        LOGGER.log(Level.WARNING, "Failed to commit offsets during partition revocation", e);
      }
    }
  }

  @Override
  public void onPartitionsAssigned(final Collection<TopicPartition> partitions) {
    if (state.get() == OffsetState.STOPPED) return;
    LOGGER.info("Partitions assigned: %s".formatted(partitions));
    partitions.forEach(partition -> {
      pendingOffsets.remove(partition);
      highestProcessedOffsets.remove(partition);
    });
  }

  @Override
  public void onPartitionsLost(final Collection<TopicPartition> partitions) {
    onPartitionsRevoked(partitions);
  }

  /**
   * Processes the command queue when partitions are being revoked from this consumer.
   *
   * <p>This method iterates through all commands in the queue and filters out any references to
   * revoked partitions. For offset commit commands, it preserves the command if it contains offsets
   * for partitions still assigned to this consumer, while removing references to revoked
   * partitions.
   *
   * @param partitions partitions being revoked from this consumer
   */
  private void drainCommandQueue(Collection<TopicPartition> partitions) {
    if (commandQueue == null || commandQueue.isEmpty()) return;

    LOGGER.info("Draining command queue for revoked partitions");

    final var remainingCommands = new ArrayList<ConsumerCommand>();

    IntStream
      .iterate(commandQueue.size(), size -> size > 0, size -> size - 1)
      .mapToObj(size -> commandQueue.poll())
      .takeWhile(Objects::nonNull)
      .forEachOrdered(currentCmd -> {
        final var cmdOffsets = currentCmd.getOffsets();
        if (cmdOffsets != null && !cmdOffsets.isEmpty()) {
          final var filteredOffsets = cmdOffsets
            .entrySet()
            .stream()
            .filter(entry -> !partitions.contains(entry.getKey()))
            .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

          if (!filteredOffsets.isEmpty()) remainingCommands.add(currentCmd.withOffsets(filteredOffsets));
        } else {
          remainingCommands.add(currentCmd);
        }
      });

    commandQueue.addAll(remainingCommands);
    LOGGER.info("Command queue drained, %s commands remaining".formatted(remainingCommands.size()));
  }
}
