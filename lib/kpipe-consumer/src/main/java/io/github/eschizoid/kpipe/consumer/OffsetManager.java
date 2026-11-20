package io.github.eschizoid.kpipe.consumer;

import java.util.Collection;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;

/// Decides which offsets a consumer commits. The default, [KafkaOffsetManager], commits the
/// contiguous frontier of processed records to Kafka or to an [OffsetStore]; implement this only
/// to replace how that frontier is computed or committed.
///
/// The consumer calls the methods in this order, and an implementation may rely on it:
///
/// 1. [#start()] once, before the consumer subscribes.
/// 2. [#trackOffset] for each record, on the consumer thread, before the record is dispatched.
/// 3. [#markOffsetProcessed] once a record is done for good: processed, filtered, unrouted, parked
///    in the dead-letter topic, or failed with no dead-letter topic configured. It is called from
///    worker threads, batch flush threads and the consumer thread, concurrently, so it must be
///    thread-safe. It is not called for a record whose dead-letter send failed, which stays pending
///    until the next restart or rebalance.
/// 4. The rebalance listener from [#createRebalanceListener()], on the consumer thread, inside
///    `poll`. On revoke it must commit what it owns for the revoked partitions and forget them. On
///    loss it must forget them; another consumer may already own them, so a commit to storage that
///    cannot tell, unlike Kafka, would move the new owner's offset backwards.
/// 5. [#close()], once, normally on the consumer thread before the Kafka consumer closes, or on the
///    caller's thread for a consumer that never started. `close` makes the final commit. When
///    another thread closes the consumer, [#signalClosing()] comes first, on that thread and
///    concurrently with the consumer thread, so a manager blocking the consumer thread can stop. A
///    poll loop that ends on its own goes straight to `close`.
///
/// Only the consumer thread may call the Kafka consumer. To commit from a timer, go through the
/// consumer's [CommitExecutor] (`KPipeConsumerBuilder.getCommitExecutor()`), which runs the
/// commit on that thread.
///
/// A [#trackOffset] or [#markOffsetProcessed] that throws is logged at ERROR and does not stop the
/// consumer or re-run the record, but the offset then stays pending, so the commit point holds
/// there until the next restart or rebalance.
public interface OffsetManager extends AutoCloseable {
  /// Starts the manager, before the consumer subscribes.
  void start();

  /// Records that `record` is about to be processed.
  ///
  /// @param record the record
  void trackOffset(final ConsumerRecord<byte[], byte[]> record);

  /// Records that `record` is done for good, so its offset may be committed once every earlier
  /// offset in its partition is done too.
  ///
  /// @param record the record
  void markOffsetProcessed(final ConsumerRecord<byte[], byte[]> record);

  /// The rebalance listener the consumer subscribes with. The default does nothing, which is right
  /// only for a manager that keeps no per-partition state; one that does must commit and forget
  /// revoked partitions, and forget lost ones. A wrapper around another manager must forward
  /// `onPartitionsLost` too: `ConsumerRebalanceListener`'s own default treats a loss as a revoke.
  ///
  /// @return the listener, never null
  default ConsumerRebalanceListener createRebalanceListener() {
    return new ConsumerRebalanceListener() {
      @Override
      public void onPartitionsRevoked(final Collection<TopicPartition> partitions) {}

      @Override
      public void onPartitionsAssigned(final Collection<TopicPartition> partitions) {}

      @Override
      public void onPartitionsLost(final Collection<TopicPartition> partitions) {}
    };
  }

  /// Tells the manager the consumer is closing. The consumer calls this before it wakes its
  /// thread, so a manager blocking that thread, such as one retrying a store read inside a
  /// rebalance callback, can stop. The final commit still belongs to [#close].
  default void signalClosing() {}

  /// Makes the final commit and releases the manager's resources.
  @Override
  void close();
}
