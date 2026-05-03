package org.kpipe.consumer;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.kpipe.consumer.enums.OffsetState;

/// Convenience base class for [OffsetManager] implementations that only need to customize how
/// offsets are tracked and persisted.
///
/// The full [OffsetManager] surface has nine methods, but for most custom backends (PostgreSQL,
/// Redis, an HTTP service, etc.) only [#trackOffset(ConsumerRecord)] and
/// [#markOffsetProcessed(ConsumerRecord)] are genuinely custom. The other methods either have a
/// sensible no-op default or can be derived from a small amount of shared state. This class
/// provides those defaults so subclasses can stay focused on the persistence logic.
///
/// Provided defaults:
///
/// * `start()` / `stop()` — atomic transitions over an internal [OffsetState] reference
///   ([OffsetState#CREATED] → [OffsetState#RUNNING] → [OffsetState#STOPPING] →
///   [OffsetState#STOPPED]). Override if startup or shutdown requires resource management
///   (schedulers, connection pools, etc.).
/// * `notifyCommitComplete(...)` — no-op. Override only if your implementation issues
///   asynchronous commits and needs the completion callback to settle a future.
/// * `createRebalanceListener()` — returns a no-op [ConsumerRebalanceListener]. Override if you
///   need to drain pending state or invalidate caches when partitions are revoked or assigned.
/// * `getState()` / `isRunning()` — derived from the internal state reference.
/// * `getStatistics()` — returns an empty map. Override to expose diagnostic counters.
/// * `close()` — no-op. Override to release resources held by the implementation.
///
/// Left abstract:
///
/// * [#trackOffset(ConsumerRecord)] — every backend tracks pending work differently.
/// * [#markOffsetProcessed(ConsumerRecord)] — every backend persists progress differently.
///
/// Example custom implementation:
///
/// ```java
/// public final class PostgresOffsetManager<K> extends AbstractOffsetManager<K> {
///
///   private final DataSource dataSource;
///
///   public PostgresOffsetManager(final DataSource dataSource) {
///     this.dataSource = dataSource;
///   }
///
///   @Override
///   public void trackOffset(final ConsumerRecord<K, byte[]> record) {
///     // record into an in-memory pending set keyed by TopicPartition
///   }
///
///   @Override
///   public void markOffsetProcessed(final ConsumerRecord<K, byte[]> record) {
///     // remove from pending set, UPSERT the lowest-pending offset into Postgres
///   }
/// }
/// ```
///
/// When NOT to extend this class: if you are implementing the full surface — periodic commits,
/// asynchronous commit futures, rebalance hooks, scheduler lifecycle — implement [OffsetManager]
/// directly. [KafkaOffsetManager] is the canonical example: it owns a scheduler, a command
/// queue, async commit futures, and a partition-aware rebalance listener, so inheriting no-op
/// defaults would only obscure intent.
///
/// Subclasses are expected to be thread-safe; [#trackOffset(ConsumerRecord)] and
/// [#markOffsetProcessed(ConsumerRecord)] may be invoked concurrently from many virtual threads.
///
/// @param <K> the type of the consumer record key
public abstract class AbstractOffsetManager<K> implements OffsetManager<K> {

  /// Internal state tracker shared by [#start()], [#stop()], [#getState()], and
  /// [#isRunning()]. Subclasses may read it but should not mutate it directly — go through
  /// the lifecycle methods instead.
  protected final AtomicReference<OffsetState> state = new AtomicReference<>(OffsetState.CREATED);

  /// Default constructor for subclasses. Initializes the manager in the
  /// [OffsetState#CREATED] state.
  protected AbstractOffsetManager() {
    // intentionally empty
  }

  /// Default lifecycle start: transitions [OffsetState#CREATED] to [OffsetState#RUNNING]
  /// atomically. A previously stopped manager cannot be restarted; calling `start()` after
  /// [#stop()] is a no-op (the state remains [OffsetState#STOPPED]).
  ///
  /// Override if your implementation needs to allocate resources, open connections, or
  /// schedule background tasks at startup.
  ///
  /// @return this instance for method chaining
  @Override
  public OffsetManager<K> start() {
    state.compareAndSet(OffsetState.CREATED, OffsetState.RUNNING);
    return this;
  }

  /// Default lifecycle stop: transitions [OffsetState#RUNNING] to [OffsetState#STOPPED] via
  /// [OffsetState#STOPPING]. Idempotent — calling `stop()` on an already-stopped manager is a
  /// no-op.
  ///
  /// Override if your implementation needs to flush in-flight work, cancel scheduled tasks,
  /// or release resources during shutdown.
  ///
  /// @return this instance for method chaining
  @Override
  public OffsetManager<K> stop() {
    if (state.compareAndSet(OffsetState.RUNNING, OffsetState.STOPPING)) {
      state.set(OffsetState.STOPPED);
    }
    return this;
  }

  /// Default no-op completion callback. Override only if your implementation issues
  /// asynchronous commits and needs the completion result to settle a future tracked by
  /// `commitId`.
  ///
  /// @param commitId the ID of the commit operation
  /// @param success whether the commit was successful
  @Override
  public void notifyCommitComplete(final String commitId, final boolean success) {
    // no-op: implementations that don't manage async commit futures don't need this hook
  }

  /// Default no-op [ConsumerRebalanceListener]. Most custom offset managers do not need to
  /// react to partition assignments because their state is keyed by [TopicPartition] and is
  /// safe to retain across rebalances.
  ///
  /// Override if you need to drain pending state, invalidate caches, or surrender resources
  /// when partitions are revoked or newly assigned.
  ///
  /// @return a no-op rebalance listener
  @Override
  public ConsumerRebalanceListener createRebalanceListener() {
    return new ConsumerRebalanceListener() {
      @Override
      public void onPartitionsRevoked(final java.util.Collection<TopicPartition> partitions) {
        // no-op
      }

      @Override
      public void onPartitionsAssigned(final java.util.Collection<TopicPartition> partitions) {
        // no-op
      }
    };
  }

  /// Returns the current state derived from the internal [AtomicReference].
  ///
  /// @return the current [OffsetState]
  @Override
  public OffsetState getState() {
    return state.get();
  }

  /// Returns whether the manager is currently in [OffsetState#RUNNING].
  ///
  /// @return true if running, false otherwise
  @Override
  public boolean isRunning() {
    return state.get() == OffsetState.RUNNING;
  }

  /// Returns an empty, immutable statistics map. Override to expose backend-specific counters
  /// such as pending offset counts, last commit latency, or persistence error rates.
  ///
  /// @return an empty map
  @Override
  public Map<String, Object> getStatistics() {
    return Collections.emptyMap();
  }

  /// Default no-op close. Override to release resources (connection pools, executors,
  /// schedulers) held by the implementation. The default implementation does not call
  /// [#stop()]; subclasses that need stop-on-close semantics should override and invoke
  /// `stop()` explicitly.
  @Override
  public void close() {
    // no-op: subclasses that hold resources should override
  }
}
