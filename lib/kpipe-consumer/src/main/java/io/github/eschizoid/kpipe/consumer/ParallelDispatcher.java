package io.github.eschizoid.kpipe.consumer;

import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;

/// Parallel dispatcher: every record gets its own virtual thread via
/// `Executors.newVirtualThreadPerTaskExecutor()`. No ordering guarantees within or across
/// keys; unbounded parallelism bounded only by Loom's carrier-thread capacity.
///
/// Pairs with [BackpressureController#inFlightStrategy] in [KPipeConsumer] — `pendingCount()`
/// returns the number of records currently submitted but not yet finished.
///
/// Owns its `ExecutorService` and shuts it down in [#close()] using the same `shutdown +
/// awaitTermination + shutdownNow` pattern KPipeConsumer used previously.
///
/// @param <K> the Kafka record key type
final class ParallelDispatcher<K> implements Dispatcher<K> {

  private static final Logger LOGGER = System.getLogger(ParallelDispatcher.class.getName());

  private final ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
  private final AtomicLong inFlight = new AtomicLong(0);
  private final BiConsumer<ConsumerRecord<K, byte[]>, RejectedExecutionException> rejectHandler;
  private final Duration terminationTimeout;

  /// @param rejectHandler  invoked when the executor rejects a task (typically after
  ///                       `executor.shutdown()` started). The handler is responsible for
  ///                       surfacing the rejection to the consumer's error path. Always called
  ///                       on the caller's thread (the consumer thread).
  /// @param terminationTimeout maximum time `close()` waits for in-flight tasks to finish
  ///                           before calling `shutdownNow()`
  ParallelDispatcher(
    final BiConsumer<ConsumerRecord<K, byte[]>, RejectedExecutionException> rejectHandler,
    final Duration terminationTimeout
  ) {
    this.rejectHandler = rejectHandler;
    this.terminationTimeout = terminationTimeout;
  }

  /// `processTask` must handle its own exceptions — `executor.submit(Runnable)` returns a `Future`
  /// that the dispatcher discards, so any throwable escaping `processTask.run()` is silently
  /// swallowed by the VT executor. The finally block still decrements `inFlight` and fires
  /// `onComplete` so accounting and backpressure remain honest, but the failure itself only
  /// surfaces if `processTask` routed it (e.g. through the consumer's error handler / DLQ).
  @Override
  public void dispatch(final ConsumerRecord<K, byte[]> record, final Runnable processTask, final Runnable onComplete) {
    inFlight.incrementAndGet();
    try {
      executor.submit(() -> {
        try {
          processTask.run();
        } finally {
          inFlight.decrementAndGet();
          onComplete.run();
        }
      });
    } catch (final RejectedExecutionException e) {
      inFlight.decrementAndGet();
      rejectHandler.accept(record, e);
    }
  }

  @Override
  public long pendingCount() {
    return inFlight.get();
  }

  /// `shutdownNow()` doesn't strand `inFlight`: the VT-per-task executor has no work queue, so
  /// it returns an empty list and only interrupts running tasks — and interrupt doesn't skip a
  /// `finally`, so each task still decrements. A pooled executor WOULD strand queued tasks here;
  /// `ParallelDispatcherTest.pendingCountDrainsToZeroWhenCloseInterruptsRunningTask` guards it.
  @Override
  public void close() {
    try {
      executor.shutdown();
      if (!executor.awaitTermination(terminationTimeout.toMillis(), TimeUnit.MILLISECONDS)) {
        // Log the still-in-flight count, NOT shutdownNow().size() — the latter is always 0 for
        // the queue-less VT-per-task executor, so it would falsely read "0 tasks not processed"
        // even while tasks were running. shutdownNow() interrupts those running tasks.
        final var stillInFlight = inFlight.get();
        executor.shutdownNow();
        LOGGER.log(
          Level.WARNING,
          "Executor did not terminate within {0}ms; interrupted {1} in-flight task(s)",
          terminationTimeout.toMillis(),
          stillInFlight
        );
      }
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      LOGGER.log(Level.WARNING, "Interrupted while awaiting executor termination", e);
      executor.shutdownNow();
    }
  }
}
