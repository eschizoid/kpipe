package org.kpipe.consumer;

import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

/// Represents commands that can be sent to a consumer.
///
/// These commands control the consumer's operational behavior through its lifecycle.
/// Each command is an immutable record, making them safe for concurrent use across virtual threads.
public sealed interface ConsumerCommand {
  /// Pause the consumer from processing messages.
  record Pause() implements ConsumerCommand {}

  /// Resume a paused consumer.
  record Resume() implements ConsumerCommand {}

  /// Close the consumer and release resources.
  record Close() implements ConsumerCommand {}

  /// Track an offset in the OffsetManager.
  ///
  /// @param record the record whose offset should be tracked
  record TrackOffset(ConsumerRecord<?, ?> record) implements ConsumerCommand {}

  /// Mark an offset as processed in the OffsetManager.
  ///
  /// @param record the record whose offset has been processed
  record MarkOffsetProcessed(ConsumerRecord<?, ?> record) implements ConsumerCommand {}

  /// Commit offsets to Kafka.
  ///
  /// @param offsets  the offsets to commit
  /// @param commitId the commit ID for tracking completion
  record CommitOffsets(Map<TopicPartition, OffsetAndMetadata> offsets, String commitId) implements ConsumerCommand {
    /// Returns a new CommitOffsets with the given offsets but same commitId.
    ///
    /// @param newOffsets the replacement offsets
    /// @return a new CommitOffsets instance
    public CommitOffsets withOffsets(final Map<TopicPartition, OffsetAndMetadata> newOffsets) {
      return new CommitOffsets(newOffsets, this.commitId);
    }
  }
}
