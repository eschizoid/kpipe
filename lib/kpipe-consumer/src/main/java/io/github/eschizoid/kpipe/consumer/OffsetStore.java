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
/// processing mode.
///
/// Every offset is the next offset to read, as in Kafka's committed offsets. A restart that seeks
/// to a loaded offset reprocesses every record from that offset on, including records that had
/// finished out of order above it. Delivery is at least once: the store write is not atomic with
/// the sink.
///
/// Kafka rejects a commit from a consumer that has been evicted from the group; a store does not
/// know which consumer owns a partition. A consumer that is evicted but has not yet noticed can
/// still write its own, older frontier, which moves the stored offset backwards and causes
/// reprocessing, never a skip. A store that must rule this out needs its own fencing, such as a
/// conditional write on an owner epoch.
///
/// One manager calls its store from one thread at a time. Calls run on the Kafka consumer thread
/// during a rebalance and close, and on the manager's commit thread otherwise. A manager does not
/// coordinate with other managers, so a store instance shared between consumers is called
/// concurrently.
public interface OffsetStore {
  /// Reads the stored offsets for newly assigned partitions. The consumer seeks each returned
  /// partition to its offset. A partition left out of the result starts from Kafka's committed
  /// offset for the group, or from `auto.offset.reset` when there is none.
  ///
  /// A thrown exception means the offsets are unknown, because reading from any other position
  /// could skip records. The manager retries with a growing wait on the consumer thread, which does
  /// not poll meanwhile; a store that stays down past `max.poll.interval.ms` gets the consumer
  /// evicted from the group. Once the consumer is closing, the manager stops retrying and the
  /// failure propagates out of the rebalance callback.
  ///
  /// @param partitions the partitions just assigned to this consumer
  /// @return the next offset to read for each partition the store has an offset for
  Map<TopicPartition, Long> load(Collection<TopicPartition> partitions);

  /// Durably stores the commit frontier.
  ///
  /// A thrown exception fails this commit only. A failed periodic commit is not retried itself:
  /// the offsets stay in the manager and the next commit carries the then-current frontier. A
  /// failed commit on revoke or close is the partition's last, so the stored offset stays where the
  /// previous commit left it. Either way a lost commit means records are reprocessed after a
  /// restart, never skipped.
  ///
  /// @param offsets the next offset to read for each partition, never empty
  void commit(Map<TopicPartition, Long> offsets);
}
