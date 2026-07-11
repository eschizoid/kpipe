package io.github.eschizoid.kpipe.consumer;

import java.util.Collection;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;

/// Interface for managing Kafka consumer offsets.
///
/// This allows for different offset management strategies, such as committing to Kafka
/// or using external storage.
public interface OffsetManager extends AutoCloseable {
  /// Starts the offset manager.
  /// @return The started OffsetManager instance
  OffsetManager start();

  /// Stops the offset manager.
  /// @return The stopped OffsetManager instance
  OffsetManager stop();

  /// Tracks an offset that is about to be processed.
  /// @param record The consumer record whose offset is being tracked
  void trackOffset(final ConsumerRecord<byte[], byte[]> record);

  /// Marks an offset as successfully processed.
  /// @param record The consumer record whose offset is marked as processed
  void markOffsetProcessed(final ConsumerRecord<byte[], byte[]> record);

  /// Notifies the offset manager that a commit operation has completed.
  /// @param commitId The ID of the commit operation
  /// @param success Whether the commit was successful
  void notifyCommitComplete(final String commitId, final boolean success);

  /// Creates a rebalance listener for this offset manager.
  ///
  /// Default implementation is a no-op — most custom offset stores key their state by
  /// [TopicPartition] and can retain it across rebalances without any action. Override only
  /// to drain pending state, invalidate caches, or surrender resources on revoke/assign.
  ///
  /// @return a [ConsumerRebalanceListener] (never null)
  default ConsumerRebalanceListener createRebalanceListener() {
    return new ConsumerRebalanceListener() {
      @Override
      public void onPartitionsRevoked(final Collection<TopicPartition> partitions) {
        // no-op
      }

      @Override
      public void onPartitionsAssigned(final Collection<TopicPartition> partitions) {
        // no-op
      }
    };
  }

  /// Gets the current operational state of the offset manager.
  /// @return The current OffsetState
  OffsetState getState();

  /// Checks if the offset manager is currently running.
  /// @return True if the offset manager is running, false otherwise
  boolean isRunning();

  /// Gets statistics about the offset manager's performance and state.
  /// @return A map containing statistics
  Map<String, Object> getStatistics();

  @Override
  void close();
}
