package io.github.eschizoid.kpipe.consumer;

import java.util.Map;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

/// Commands that must execute on the consumer thread because they touch the underlying
/// `KafkaConsumer` (`pause`/`resume`/`commitSync`) or signal a state transition (`close`).
///
/// Offset tracking + per-record marking deliberately do NOT go through this queue — they call
/// `OffsetManager` directly from whichever thread finishes processing, since that manager's state
/// is already concurrent-map-backed and thread-safe. Routing them through here would just add a
/// queue hop and risk unbounded growth under PARALLEL load.
public sealed interface ConsumerCommand {
  /// Pause the consumer from processing messages.
  record Pause() implements ConsumerCommand {}

  /// Resume a paused consumer.
  record Resume() implements ConsumerCommand {}

  /// Close the consumer and release resources.
  record Close() implements ConsumerCommand {}

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
