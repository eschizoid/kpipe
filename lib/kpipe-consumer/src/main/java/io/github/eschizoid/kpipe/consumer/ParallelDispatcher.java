package io.github.eschizoid.kpipe.consumer;

import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;

/// Parallel dispatcher: every record gets its own thread, virtual by default via
/// `Thread.ofVirtual().factory()`. No ordering guarantees within or across keys, and
/// parallelism is unbounded — with the default virtual threads that is bounded in practice
/// only by Loom's carrier-thread capacity, but a caller supplying a platform-thread factory
/// through the test seam gets one operating-system thread per in-flight record instead.
/// Backpressure, not this class, is what keeps that number sane.
///
/// Pairs with [BackpressureController#inFlightStrategy] in [KPipeConsumer] — `drainableCount()`
/// returns the number of records currently submitted but not yet finished.
///
/// Creates and owns its `ExecutorService`, shut down in [#close()] using the same `shutdown +
/// awaitTermination + shutdownNow` pattern KPipeConsumer used previously.
final class ParallelDispatcher implements Dispatcher {

  private static final Logger LOGGER = System.getLogger(ParallelDispatcher.class.getName());

  private final ExecutorService executor;
  private final AtomicLong inFlight = new AtomicLong(0);
  private final BiConsumer<ConsumerRecord<byte[], byte[]>, RejectedExecutionException> rejectHandler;
  private final Duration terminationTimeout;

  /// @param rejectHandler  invoked when the executor rejects a task (typically after
  ///                       `executor.shutdown()` started). The handler is responsible for
  ///                       surfacing the rejection to the consumer's error path. Always called
  ///                       on the caller's thread (the consumer thread).
  /// @param terminationTimeout maximum time `close()` waits for in-flight tasks to finish
  ///                           before calling `shutdownNow()`
  ParallelDispatcher(
    final BiConsumer<ConsumerRecord<byte[], byte[]>, RejectedExecutionException> rejectHandler,
    final Duration terminationTimeout
  ) {
    this(rejectHandler, terminationTimeout, Thread.ofVirtual().factory());
  }

  /// Test seam: supplies the thread factory rather than pinning virtual threads.
  ///
  /// The JDK idles the VirtualThread carrier pool out on a 30-second schedule, so a
  /// scheduler that waits for every thread to reach a completed state pays that once per
  /// iteration. The in-flight accounting is a property of the increment/decrement protocol
  /// rather than of the thread kind, so platform threads exercise it equally.
  ///
  /// Takes a factory rather than an executor deliberately. `close()` relies on this executor
  /// having no work queue — that is what makes `shutdownNow()` return nothing and leaves no task
  /// stranded with `inFlight` already incremented. Building the executor here keeps that
  /// guarantee out of a caller's hands: a queueing executor would leave `drainableCount()`
  /// permanently non-zero after `close()`, and that value drives the backpressure watermark and
  /// the drain wait. It also settles ownership, since the dispatcher only ever closes what it
  /// created.
  ///
  /// The factory must produce daemon threads, for the same reason: `close()` reaches
  /// `shutdownNow()`, which interrupts, and a task that ignores interruption outlives it.
  ///
  /// @param rejectHandler      invoked when the executor refuses a record during shutdown
  /// @param terminationTimeout maximum time `close()` waits for in-flight tasks to finish
  /// @param threadFactory      creates one thread per dispatched record; must produce daemon threads
  ParallelDispatcher(
    final BiConsumer<ConsumerRecord<byte[], byte[]>, RejectedExecutionException> rejectHandler,
    final Duration terminationTimeout,
    final ThreadFactory threadFactory
  ) {
    this.rejectHandler = rejectHandler;
    this.terminationTimeout = terminationTimeout;
    this.executor = Executors.newThreadPerTaskExecutor(requireDaemonFactory(threadFactory));
  }

  /// `processTask` is expected to handle its own exceptions (the consumer's per-record error
  /// handling / DLQ runs inside it). As a safety net for a contract violation, an outer
  /// `catch (Throwable)` logs anything that still escapes `processTask.run()` at ERROR rather than
  /// letting the discarded `Future` from `executor.submit(...)` swallow it silently — mirroring
  /// [KeyOrderedDispatcher]'s drain loop. The finally block always decrements `inFlight` and fires
  /// `onComplete` (itself guarded) so accounting and backpressure stay honest.
  @Override
  public void dispatch(
    final ConsumerRecord<byte[], byte[]> record,
    final Runnable processTask,
    final Runnable onComplete
  ) {
    inFlight.incrementAndGet();
    try {
      executor.submit(() -> {
        try {
          try {
            processTask.run();
          } finally {
            inFlight.decrementAndGet();
            try {
              onComplete.run();
            } catch (final RuntimeException e) {
              LOGGER.log(Level.WARNING, "onComplete callback threw", e);
            }
          }
        } catch (final Throwable t) {
          LOGGER.log(
            Level.ERROR,
            "Dispatched record task threw; the record's own error handling should have caught it",
            t
          );
        }
      });
    } catch (final RejectedExecutionException e) {
      inFlight.decrementAndGet();
      rejectHandler.accept(record, e);
    }
  }

  @Override
  public long drainableCount() {
    return inFlight.get();
  }

  /// Rejects a factory that produces non-daemon threads, which is a contract the constructor is
  /// the only place able to enforce. The probe must come back unstarted, which is checked here,
  /// so it costs an object and no operating-system resource.
  ///
  /// @param factory the candidate thread factory
  /// @return the same factory, when it produces daemon threads
  /// @throws IllegalArgumentException when it does not
  static ThreadFactory requireDaemonFactory(final ThreadFactory factory) {
    if (factory == null) {
      throw new IllegalArgumentException("threadFactory must not be null");
    }
    // ThreadFactory.newThread is specified to return null when it declines to create a
    // thread, so the probe has to handle that rather than dereference it.
    final var probe = factory.newThread(() -> {});
    if (probe == null) {
      throw new IllegalArgumentException(
        "threadFactory refused to create a thread, so its daemon status cannot be established"
      );
    }
    if (probe.getState() != Thread.State.NEW) {
      throw new IllegalArgumentException(
        "threadFactory must return unstarted threads; the daemon probe would otherwise run work and the "
          + "dispatcher would not own the thread's lifecycle"
      );
    }
    if (!probe.isDaemon()) {
      throw new IllegalArgumentException(
        "threadFactory must produce daemon threads: close() reaches shutdownNow(), which interrupts, "
          + "and a task that ignores interruption would keep the JVM alive"
      );
    }
    return factory;
  }

  /// `shutdownNow()` doesn't strand `inFlight`: a thread-per-task executor starts every task as
  /// it is submitted, so there is no queue of accepted-but-never-run tasks to abandon — it only
  /// interrupts tasks already running, and interrupt doesn't skip a `finally`, so each one still
  /// decrements. A queueing executor WOULD strand them here, which is why the seam takes a
  /// factory and builds the executor rather than accepting one.
  /// `ParallelDispatcherTest.drainableCountDrainsToZeroWhenCloseInterruptsRunningTask` guards it.
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
