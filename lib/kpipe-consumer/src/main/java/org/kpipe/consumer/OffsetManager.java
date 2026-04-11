package org.kpipe.consumer;

import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.kpipe.consumer.enums.OffsetState;

/// Interface for managing Kafka consumer offsets.
///
/// This allows for different offset management strategies, such as committing to Kafka
/// or using external storage.
///
/// @param <K> The type of the record key
/// @param <V> The type of the record value
public interface OffsetManager<K, V> extends AutoCloseable {
  /// Starts the offset manager.
  /// @return The started OffsetManager instance
  OffsetManager<K, V> start();

  /// Stops the offset manager.
  /// @return The stopped OffsetManager instance
  OffsetManager<K, V> stop();

  /// Tracks an offset that is about to be processed.
  /// @param record The consumer record whose offset is being tracked
  void trackOffset(final ConsumerRecord<K, V> record);

  /// Marks an offset as successfully processed.
  /// @param record The consumer record whose offset is marked as processed
  void markOffsetProcessed(final ConsumerRecord<K, V> record);

  /// Notifies the offset manager that a commit operation has completed.
  /// @param commitId The ID of the commit operation
  /// @param success Whether the commit was successful
  void notifyCommitComplete(final String commitId, final boolean success);

  /// Creates a rebalance listener for this offset manager.
  /// @return A ConsumerRebalanceListener instance
  ConsumerRebalanceListener createRebalanceListener();

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
