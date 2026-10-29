package io.github.eschizoid.kpipe.consumer;

import java.util.Collection;
import java.util.Map;
import org.apache.kafka.common.TopicPartition;

/// Where [KafkaOffsetManager] keeps committed offsets when they live outside Kafka, such as a
/// database table or a DynamoDB global table.
///
/// The manager still decides what is safe to commit. It hands the store only the commit frontier:
/// for each partition, the lowest offset that has not finished processing, so every offset below
/// it has. A store never sees an offset that a still-running record could invalidate, whatever the
/// processing mode, so an implementation is plain I/O: write the map, read it back.
///
/// Every offset is the next offset to read, as in Kafka's committed offsets. A restart that seeks
/// to a loaded offset reprocesses every record that had not finished.
///
/// The manager calls the store from one thread at a time, so an implementation does not need its
/// own locking. Calls run on the Kafka consumer thread during a rebalance and close, and on the
/// manager's commit thread otherwise; a slow store delays polling only during a rebalance.
public interface OffsetStore {
  /// Reads the stored offsets for newly assigned partitions. The consumer seeks each returned
  /// partition to its offset. A partition left out of the result starts from Kafka's committed
  /// offset for the group, or from `auto.offset.reset` when there is none.
  ///
  /// A thrown exception means the offsets are unknown. The manager retries until the load succeeds
  /// or the consumer closes, because reading from any other position could skip records.
  ///
  /// @param partitions the partitions just assigned to this consumer
  /// @return the next offset to read for each partition the store has an offset for
  Map<TopicPartition, Long> load(Collection<TopicPartition> partitions);

  /// Durably stores the commit frontier.
  ///
  /// A thrown exception fails this commit only. The offsets stay in the manager and the next commit
  /// carries the then-current frontier, so a lost commit means records are reprocessed after a
  /// restart, never skipped.
  ///
  /// @param offsets the next offset to read for each partition, never empty
  void commit(Map<TopicPartition, Long> offsets);
}
