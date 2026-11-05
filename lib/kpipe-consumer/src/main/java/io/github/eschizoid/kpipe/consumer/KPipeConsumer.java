package io.github.eschizoid.kpipe.consumer;

import io.github.eschizoid.kpipe.producer.KPipeProducer;
import io.github.eschizoid.kpipe.registry.MessagePipeline;
import io.github.eschizoid.kpipe.tracing.Tracer;
import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;
import java.util.stream.Collectors;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.WakeupException;

/// A functional-style Kafka consumer that processes records using a provided function.
///
/// This consumer provides:
///
/// * A simple functional interface for message processing
/// * Built-in retry logic with configurable backoff
/// * Support for sequential or parallel message processing
/// * Thread-safe offset management for concurrent processing scenarios
/// * Customizable error handling
/// * Message sink support for processed records
/// * Built-in metrics tracking
/// * Graceful shutdown handling
///
/// The offset management features:
///
/// * Thread-safe tracking of offsets for concurrent processing
/// * Ensures only contiguous completed offsets are committed
/// * Commits offsets periodically at a configurable interval
/// * Properly handles consumer rebalancing events
/// * Prevents data loss during parallel processing
/// * Handles non-sequential offset completion safely
///
/// Offset commits belong to an [OffsetManager]: a [KafkaOffsetManager] by default, or the one
/// supplied to the builder. Kafka's auto-commit is always disabled, because it would commit polled
/// offsets before their records finish.
///
/// Example usage:
///
/// ```java
/// final var registry = new MessageProcessorRegistry();
/// final var consoleSinkKey = RegistryKey.json("jsonConsole");
/// registry.registerSink(consoleSinkKey, new JsonConsoleSink<>());
///
/// final var pipeline = registry.pipeline(JsonFormat.INSTANCE)
///     .toSink(consoleSinkKey)
///     .build();
///
/// final var consumer = KPipeConsumer.builder()
///     .withProperties(kafkaProps)
///     .withTopic("example-topic")
///     .withPipeline(pipeline)
///     .withRetry(3, Duration.ofSeconds(1))
///     .build();
///
/// consumer.start();
/// consumer.awaitShutdown();   // blocks until close() completes
/// ```
///
public class KPipeConsumer implements AutoCloseable {

  private static final Logger LOGGER = System.getLogger(KPipeConsumer.class.getName());

  private final Queue<ConsumerCommand> commandQueue;
  private final Consumer<byte[], byte[]> kafkaConsumer;
  private final Set<String> topics;
  private final Duration pollTimeout;
  private final AtomicReference<Thread> consumerThread = new AtomicReference<>();
  private final Duration waitForMessagesTimeout;
  private final Duration threadTerminationTimeout;
  private final OffsetManager offsetManager;
  private final ConsumerRebalanceListener rebalanceListener;
  private final AtomicReference<ConsumerState> state = new AtomicReference<>(ConsumerState.CREATED);
  /// Guards [#releaseConstructedResources] so it runs exactly once no matter which path ends
  /// the consumer: external `close()`, the never-started fast path, or the consumer thread
  /// terminating on its own (uncaught exception / interruption). Without this, a self-
  /// terminating consumer thread would set state=CLOSED and a later `close()` would short-
  /// circuit, leaking the dispatcher / scheduler / offset manager / DLQ producer / batch
  /// wrappers for the JVM's lifetime.
  private final AtomicBoolean resourcesReleased = new AtomicBoolean(false);
  private final ProcessingMode processingMode;
  private final ConsumerCounters counters = new ConsumerCounters();
  private final GuardedMetricsExporter exporter;
  private final String consumerGroup;
  /// Owns the per-mode dispatch strategy and the in-flight counter for non-sequential modes.
  /// Constructed once in the consumer constructor from [#processingMode]; closed in `close()`
  /// before `offsetManager.close()` so any in-flight records get their offsets marked before
  /// the manager flushes its final commit.
  private final Dispatcher dispatcher;
  /// Composes pause arbitration + backpressure decision + circuit-breaker state machine. The
  /// underlying decision modules ([BackpressureController], [CircuitBreakerController]) remain
  /// public + testable on their own; this controller owns the side-effect choreography and
  /// dispatches pause transitions through a PauseLifecycleHook that points back at
  /// `internalPause` / `internalResume`, and metric events through a HealthMetricsObserver bound
  /// to the counters.
  private final ConsumerHealthController health;

  private final KPipeProducer<byte[], byte[]> kpipeProducer;

  /// Per-record processing engine: deserialize → operators → sink → mark/DLQ, including the
  /// retry loop, span handling, and the batch-route buffering. The consumer only tracks,
  /// dispatches, and delegates; see [RecordProcessor] for the per-record semantics.
  private final RecordProcessor recordProcessor;

  private final Map<String, BatchPipelineWrapper<?>> batchWrappers;

  // ── Folded-in lifecycle host concerns (former KPipeRunner) ──
  /// Released by `close()` so callers blocked in [#awaitShutdown(Duration)] unblock when the
  /// consumer reaches CLOSED. Initialised once so the latch is stable across reconfiguration.
  private final CountDownLatch shutdownLatch = new CountDownLatch(1);
  /// Writes the metrics log line every [#metricsLogInterval]. Started at the end of `start()`
  /// when the interval is set; interrupted at the start of `close()`.
  private volatile Thread metricsLogThread;
  private final Duration metricsLogInterval;
  /// JVM shutdown hook (when `useShutdownHook=true`); kept so `close()` can remove it explicitly.
  /// Without this, every constructed consumer accumulates a hook in the JVM's shutdown-hook set
  /// for the lifetime of the process — a real leak in hosts that build/close many consumers.
  private volatile Thread shutdownHookThread;
  /// Shared virtual-thread scheduler. Owns batch-pipeline age ticks AND the circuit-breaker
  /// probe timer. Lazily created when either feature is configured; otherwise stays null.
  private final ScheduledExecutorService scheduler;

  /// Represents an error that occurred during record processing. Contains the original record,
  /// the exception that was thrown, and the number of retry attempts made.
  ///
  /// @param record the Kafka record that failed processing
  /// @param exception the exception that occurred during processing
  /// @param retryCount the number of retry attempts made
  public record ProcessingError(ConsumerRecord<byte[], byte[]> record, Exception exception, int retryCount) {}

  /// A functional interface for handling processing errors.
  @FunctionalInterface
  public interface ErrorHandler extends java.util.function.Consumer<ProcessingError> {}

  /// Creates a new builder for constructing [KPipeConsumer] instances.
  ///
  /// Bytes at the boundary, both sides: keys and values are always `byte[]`. Format SerDe lives
  /// inside the pipeline; key interpretation is the caller's concern in error handlers and sinks.
  /// Kafka keys are nullable, so null-guard the decode:
  /// `record.key() == null ? null : new String(record.key(), UTF_8)`.
  ///
  /// @return a new builder instance
  public static KPipeConsumerBuilder builder() {
    return new KPipeConsumerBuilder();
  }

  /// Creates a new KPipeConsumer using the provided builder.
  ///
  /// @param builder the builder containing the consumer configuration
  public KPipeConsumer(final KPipeConsumerBuilder builder) {
    // The constructor opens resources in sequence: the Kafka consumer, the dispatcher (PARALLEL
    // owns a virtual-thread executor), an optional built-in DLQ producer, an optional scheduler,
    // the batch wrappers, and a JVM shutdown hook. If any step after the first open throws — e.g.
    // addShutdownHook while the JVM is already shutting down, or a user-supplied
    // metrics/tracer/circuit-breaker whose construction work throws — everything already opened
    // would leak for the JVM lifetime. Wrap construction so any throwable triggers reverse-order
    // cleanup of whatever was opened, then rethrow with cleanup failures suppressed onto it.
    try {
      this.kafkaConsumer =
        builder.consumerProvider != null
          ? builder.consumerProvider.get()
          : new KafkaConsumer<>(Objects.requireNonNull(builder.kafkaProps));
      this.topics = Set.copyOf(Objects.requireNonNull(builder.topics, "Topics must be set"));
      // `build()` sets `topics` to the UNION of regular + batch routes for Kafka subscription, so
      // the homogeneous branch must filter out batch-route topics to avoid replicating the regular
      // pipeline onto a topic that's owned by a batch wrapper.
      final Map<String, MessagePipeline<?>> pipelines;
      if (builder.pipelinesPerTopic != null) {
        pipelines = builder.pipelinesPerTopic;
      } else if (builder.pipeline != null) {
        final var map = new LinkedHashMap<String, MessagePipeline<?>>(builder.topics.size());
        for (final var t : builder.topics) if (!builder.batchSpecs.containsKey(t)) map.put(t, builder.pipeline);
        pipelines = Map.copyOf(map);
      } else {
        pipelines = Map.of();
      }
      this.pollTimeout = Objects.requireNonNull(builder.pollTimeout);
      this.processingMode = builder.processingMode;
      this.waitForMessagesTimeout = builder.waitForMessagesTimeout;
      this.threadTerminationTimeout =
        builder.threadTerminationTimeout != null
          ? builder.threadTerminationTimeout
          : ConsumerDefaults.threadTermination(
              builder.processingMode,
              builder.kpipeProducer != null || builder.deadLetterTopic != null
            );
      this.dispatcher = switch (this.processingMode) {
        case SEQUENTIAL -> new SequentialDispatcher();
        case PARALLEL -> new ParallelDispatcher(this::handleParallelRejection, builder.executorTerminationTimeout);
        case KEY_ORDERED -> new KeyOrderedDispatcher(builder.keyOrderedMaxKeys);
      };
      this.offsetManager =
        builder.offsetManager != null
          ? builder.offsetManager
          : builder.offsetManagerProvider != null
            ? builder.offsetManagerProvider.apply(this.kafkaConsumer)
            : null;
      this.commandQueue = builder.commandQueue != null ? builder.commandQueue : new ConcurrentLinkedQueue<>();
      this.rebalanceListener =
        builder.rebalanceListener != null
          ? builder.rebalanceListener
          : (this.offsetManager != null ? this.offsetManager.createRebalanceListener() : null);

      final var bp = (
        builder.backpressureController != null
          ? builder.backpressureController
          : new BackpressureController(
              BackpressureController.DEFAULT_HIGH_WATERMARK,
              BackpressureController.DEFAULT_LOW_WATERMARK,
              null
            )
      ).withStrategy(
        this.processingMode == ProcessingMode.SEQUENTIAL
          ? BackpressureController.lagStrategy()
          : BackpressureController.inFlightStrategy(this::backpressureLoad)
      );

      final var tracer = builder.tracer != null ? builder.tracer : Tracer.noop();

      final var deadLetterTopic = builder.deadLetterTopic;
      this.kpipeProducer =
        builder.kpipeProducer != null
          ? builder.kpipeProducer
          : deadLetterTopic != null
            ? KPipeProducer.<byte[], byte[]>builder().withProperties(builder.kafkaProps).withTracer(tracer).build()
            : null;

      final var needScheduler = !builder.batchSpecs.isEmpty() || builder.circuitBreakerController != null;
      if (needScheduler) {
        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
          final var t = Thread.ofVirtual().unstarted(r);
          t.setName("kpipe-scheduler");
          return t;
        });
      } else {
        this.scheduler = null;
      }

      this.metricsLogInterval = builder.metricsLogInterval;
      final var useShutdownHook = builder.useShutdownHook;
      if (useShutdownHook) {
        this.shutdownHookThread = new Thread(this::close, "kpipe-shutdown-hook");
        Runtime.getRuntime().addShutdownHook(this.shutdownHookThread);
      }

      if (builder.backpressureController == null) {
        LOGGER.log(
          Level.INFO,
          "No backpressure configured, using default {0} strategy (high={1}, low={2})",
          bp.getMetricName(),
          bp.highWatermark(),
          bp.lowWatermark()
        );
      }

      this.exporter = new GuardedMetricsExporter(builder.metricsExporter);
      this.consumerGroup = builder.kafkaProps.getProperty("group.id");

      this.health = new ConsumerHealthController(
        bp,
        builder.circuitBreakerController,
        this.scheduler,
        new ConsumerHealthController.PauseLifecycleHook() {
          @Override
          public void onPause() {
            internalPause();
          }

          @Override
          public void onResume() {
            internalResume();
          }
        },
        new ConsumerHealthController.HealthMetricsObserver() {
          @Override
          public void onBackpressurePause() {
            counters.backpressurePaused();
          }

          @Override
          public void onBackpressureTimeMs(final long ms) {
            counters.backpressureResumed();
          }

          @Override
          public void onCircuitBreakerTrip() {}

          @Override
          public void onCircuitBreakerStateChange(final CircuitBreakerState state) {
            counters.circuitBreakerChanged(state);
          }

          @Override
          public void onCircuitBreakerTimeOpenMs(final long ms) {}
        }
      );

      // The processing engine owns the per-record unit (retries, span handling, DLQ-or-mark,
      // batch buffering) and creates the batch wrappers so their flush callbacks live next to
      // the per-record error path they mirror. The consumer keeps the wrapper map for
      // start/drain/in-flight bookkeeping.
      this.recordProcessor = new RecordProcessor(
        pipelines,
        builder.batchSpecs.values(),
        this.scheduler,
        this.offsetManager,
        this.health,
        exporter,
        this.counters,
        builder.errorHandler,
        deadLetterTopic,
        this.kpipeProducer,
        tracer,
        builder.maxRetries,
        builder.retryBackoff,
        this::isRunning,
        this::wakeIfBackpressureDrained
      );
      this.batchWrappers = recordProcessor.batchWrappers();
    } catch (final Throwable t) {
      closePartiallyConstructed(t);
      throw t;
    }
  }

  /// Closes whatever the constructor managed to open before a later init step threw, in reverse
  /// of the open order — shutdown hook, scheduler, producer, batch wrappers, dispatcher, Kafka
  /// consumer. Each field is read off `this` (a partially-constructed instance leaves the
  /// not-yet-assigned ones at their default `null`), so every step is null-guarded. Cleanup
  /// failures are suppressed onto the original throwable rather than masking it. Mirrors the
  /// ownership rules of `releaseConstructedResources` / the consumer-thread teardown: the Kafka
  /// consumer and the DLQ producer are closed regardless of whether they were supplied or built
  /// internally, because once handed to the consumer they are consumer-owned.
  ///
  /// Reading the `final` fields here is legal precisely because this runs in a *separate method*,
  /// not inline in the constructor — definite-assignment analysis only guards in-constructor reads,
  /// so a blank-final that the constructor never reached simply reads its default `null`. Don't
  /// "fix" the apparent unassigned-final read by dropping `final`; the null-guard is the contract.
  private void closePartiallyConstructed(final Throwable original) {
    if (shutdownHookThread != null) {
      try {
        Runtime.getRuntime().removeShutdownHook(shutdownHookThread);
      } catch (final IllegalStateException e) {
        // JVM already shutting down — nothing to remove. Ignore.
      } catch (final RuntimeException e) {
        original.addSuppressed(e);
      } finally {
        shutdownHookThread = null;
      }
    }
    if (scheduler != null) {
      try {
        scheduler.shutdownNow();
      } catch (final RuntimeException e) {
        original.addSuppressed(e);
      }
    }
    if (kpipeProducer != null) {
      try {
        kpipeProducer.close(ConsumerDefaults.PRODUCER_CLOSE);
      } catch (final Exception e) {
        original.addSuppressed(e);
      }
    }
    if (batchWrappers != null) {
      for (final var wrapper : batchWrappers.values()) {
        try {
          wrapper.close();
        } catch (final Exception e) {
          original.addSuppressed(e);
        }
      }
    }
    if (dispatcher != null) {
      try {
        dispatcher.close();
      } catch (final Exception e) {
        original.addSuppressed(e);
      }
    }
    if (kafkaConsumer != null) {
      try {
        kafkaConsumer.close(CloseOptions.timeout(ConsumerDefaults.KAFKA_CLIENT_CLOSE));
      } catch (final Exception e) {
        original.addSuppressed(e);
      }
    }
  }

  /// Blocks until `dispatcher.drainableCount()` reaches zero, or `timeout` elapses, or the
  /// calling thread is interrupted. A `true` return means "active processing is done, safe to
  /// tear down" — buffered batches are flushed + committed by the teardown that follows.
  ///
  /// @param timeout maximum time to wait
  /// @return `true` if active processing drained in time, else `false`
  public boolean waitForInFlightDrain(final Duration timeout) {
    if (dispatcher.drainableCount() == 0) return true;
    final var deadline = System.nanoTime() + timeout.toNanos();
    final var pollNanos = Math.min(timeout.toNanos() / 10, Duration.ofMillis(500).toNanos());
    final var pollMs = Math.max(1, pollNanos / 1_000_000);
    try {
      while (System.nanoTime() < deadline) {
        if (dispatcher.drainableCount() == 0) return true;
        //noinspection BusyWait — deadline-bounded drain poll, not a spin
        Thread.sleep(pollMs);
      }
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      return false;
    }
    return dispatcher.drainableCount() == 0;
  }

  /// Runs on the consumer thread, at the top of its `finally`, before any teardown. The poll
  /// loop exits the moment `state` becomes CLOSING, so without this the finally would tear the
  /// dispatcher + offset manager down while in-flight workers are still finishing. We keep
  /// pumping the command queue (so any pending `CommitOffsets` triggered by the scheduler land on
  /// the still-open Kafka consumer) and wait for the dispatcher's pending work to settle, bounded
  /// by `waitForMessagesTimeout`. Finishing workers mark their offsets directly on the
  /// thread-safe `OffsetManager`, so those don't need the command queue. A final
  /// `processCommands()` flushes anything the last workers enqueued. Only the consumer thread may
  /// touch the Kafka consumer, so this must run here, not on the close() thread. Waits on
  /// `dispatcher.drainableCount()`; buffered batch records are flushed by each
  /// `BatchPipelineWrapper.close()` in `releaseConstructedResources()` right after this returns.
  private void drainInFlightBeforeTeardown() {
    if (waitForMessagesTimeout.toMillis() > 0) {
      final var deadline = System.nanoTime() + waitForMessagesTimeout.toNanos();
      while (dispatcher.drainableCount() > 0 && System.nanoTime() < deadline) {
        processCommands();
        LockSupport.parkNanos(Duration.ofMillis(5).toNanos());
      }
    }
    processCommands();
  }

  /// Blocks indefinitely until [#close()] returns. Use [#awaitShutdown(Duration)] for a bounded
  /// wait that returns a `boolean` instead of throwing.
  ///
  /// @throws InterruptedException if the calling thread is interrupted while waiting; the
  ///     interrupt flag is restored before throwing
  public void awaitShutdown() throws InterruptedException {
    try {
      shutdownLatch.await();
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      throw e;
    }
  }

  /// Blocks up to `timeout` for [#close()] to finish.
  ///
  /// @param timeout maximum time to wait
  /// @return `true` if shutdown completed within `timeout`, `false` on timeout or interruption
  public boolean awaitShutdown(final Duration timeout) {
    Objects.requireNonNull(timeout, "timeout cannot be null");
    try {
      return shutdownLatch.await(timeout.toMillis(), TimeUnit.MILLISECONDS);
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      return false;
    }
  }

  /// Initiates a graceful shutdown overriding the builder's `waitForMessagesTimeout`. Use when the
  /// host wants to choose the drain budget at shutdown time (e.g. a signal handler that knows how
  /// much time it has left). Returns whether the in-flight drain completed within `timeout`.
  ///
  /// @param timeout maximum time to wait for in-flight records to drain
  /// @return `true` if the drain finished cleanly, `false` if records remained when shutdown
  ///     forced through
  public boolean shutdownGracefully(final Duration timeout) {
    Objects.requireNonNull(timeout, "timeout cannot be null");
    // Snapshot state once. CLOSED → nothing to drain, already done.
    // CLOSING → another caller is mid-shutdown; await their completion and report the drain
    // they observed rather than calling pause()/close() on an already-shutting consumer.
    final var snapshot = state.get();
    if (snapshot == ConsumerState.CLOSED) return true;
    if (snapshot == ConsumerState.CLOSING) return awaitShutdown(timeout) && backpressureLoad() == 0;
    pause();
    final var drained = waitForInFlightDrain(timeout);
    close();
    return drained;
  }

  /// Starts the thread that logs a metrics line every [#metricsLogInterval], when one is set.
  /// It is a virtual thread, so it never keeps the JVM alive.
  private void startMetricsLogThread() {
    if (metricsLogInterval == null) return;
    metricsLogThread = Thread.ofVirtual()
      .name("kpipe-metrics-log")
      .start(() -> {
        try {
          while (state.get() != ConsumerState.CLOSED) {
            Thread.sleep(metricsLogInterval);
            if (state.get() == ConsumerState.RUNNING || state.get() == ConsumerState.PAUSED) logMetrics();
          }
        } catch (final InterruptedException e) {
          Thread.currentThread().interrupt();
        }
      });
  }

  /// Stops the metrics log thread, if it runs.
  private void stopMetricsLogThread() {
    final var t = metricsLogThread;
    if (t == null) return;
    t.interrupt();
    try {
      t.join(Duration.ofSeconds(1));
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  private void logMetrics() {
    try {
      MetricsLog.LOGGER.log(
        Level.INFO,
        MetricsLog.line(topics, consumerGroup, metrics(), health.circuitBreakerState())
      );
    } catch (final RuntimeException e) {
      LOGGER.log(Level.WARNING, "Could not log the consumer's metrics", e);
    }
  }

  /// Starts the consumer thread and begins consuming messages from the configured topic. The
  /// consumer will poll for records and process them asynchronously in virtual threads.
  ///
  /// @throws IllegalStateException if the consumer has already been started or was previously
  ///     closed
  public void start() {
    if (state.get() == ConsumerState.CLOSED) throw new IllegalStateException("Cannot restart a closed consumer");
    if (!state.compareAndSet(ConsumerState.CREATED, ConsumerState.RUNNING)) return;

    exporter.start(metricsSource());

    if (offsetManager != null) offsetManager.start();
    batchWrappers.values().forEach(BatchPipelineWrapper::start);
    if (rebalanceListener != null) kafkaConsumer.subscribe(topics, rebalanceListener);
    else kafkaConsumer.subscribe(topics);

    final var threadLabel = topics.size() == 1 ? topics.iterator().next() : "topics-%d".formatted(topics.size());
    final var thread = Thread.ofVirtual()
      .name("kafka-consumer-%s-%s".formatted(threadLabel, UUID.randomUUID().toString().substring(0, 8)))
      .uncaughtExceptionHandler((t, e) -> {
        LOGGER.log(Level.ERROR, "Uncaught exception in thread {0}", t.getName(), e);
        transitionToClosing();
      })
      .start(() -> {
        try {
          while (isRunning()) {
            processCommands();
            health.tickBackpressure(kafkaConsumer);
            if (!isRunning()) break;
            // Flush the command queue — including whatever the tick above queued — so
            // the Kafka-level pause state matches the consumer state before the poll
            // below. Both directions need it: pause must reach Kafka before a paused
            // poll, and a queued Resume before a running one, or the poll fetches
            // nothing from partitions Kafka still has paused. This sits outside the
            // `isPaused()` check because a tick-driven resume has already left PAUSED
            // by the time it gets here, so a guarded flush would skip exactly the
            // resume case.
            processCommands();
            // Recheck after this flush too, not only after the one at the top of
            // the loop. A Close drained just above sets CLOSING, so `isPaused()`
            // is false, the guard below is skipped, and with it the only other
            // check between here and the poll.
            if (!isRunning()) break;
            if (isPaused()) {
              // Defensively re-pause the full assignment: pause state is
              // per-partition and does not survive a revoke/assign cycle, so a
              // rebalance inside an earlier poll can hand this consumer new,
              // un-paused partitions.
              //
              // `Thread.interrupted()` clears the flag, so it stays on this path
              // alone: running it every iteration would consume an interrupt only
              // the paused path acts on. The `isRunning()` arm catches a close()
              // landing between the recheck above and this line.
              if (!isRunning() || Thread.interrupted()) break;
              kafkaConsumer.pause(kafkaConsumer.assignment());
            }

            // Paused or not, keep calling poll(). With every partition paused the fetch
            // returns nothing, but the call keeps the group membership alive — a consumer
            // that stops polling is evicted after max.poll.interval.ms, triggering a
            // silent rebalance — and bounds each iteration to `pollTimeout`, so the
            // `tickBackpressure` above re-evaluates the pause condition on a fixed
            // cadence. An indefinite LockSupport.park() here wedged the lag strategy
            // forever: a parked consumer's positions are frozen while the broker's end
            // offsets only grow, so lag can only drop through external events (partition
            // reassignment, retention truncation, manual offset reset) — none of which
            // produce an unpark. Records can still arrive while paused when a mid-poll
            // rebalance assigns new partitions after the re-pause above ran; the fetch
            // has already advanced the positions past them, so dropping them would lose
            // data — process them normally.
            final var records = health.heldByInFlightBackpressure()
              ? pollAfterBackpressureWait()
              : pollRecords(pollTimeout);
            sampleLag();
            if (records != null && !records.isEmpty()) processRecords(records);
          }
        } catch (final Exception e) {
          if (isRunning()) LOGGER.log(Level.WARNING, "Error in consumer thread", e);
        } finally {
          // Drain in-flight work + commands before teardown (see method docs).
          drainInFlightBeforeTeardown();
          try {
            // Release ctor resources before closing the consumer (see method docs).
            releaseConstructedResources();
          } finally {
            // Closing leaves the consumer group. On an interrupted thread Kafka cuts the
            // close short, and the group waits out the session timeout to rebalance.
            final var interruptedBeforeClose = Thread.interrupted();
            try {
              kafkaConsumer.close(CloseOptions.timeout(ConsumerDefaults.KAFKA_CLIENT_CLOSE));
              LOGGER.log(Level.INFO, "Consumer closed for topics {0}", topics);
            } catch (final Exception e) {
              LOGGER.log(Level.WARNING, "Error closing Kafka consumer", e);
            } finally {
              if (interruptedBeforeClose) Thread.currentThread().interrupt();
              state.set(ConsumerState.CLOSED);
              shutdownLatch.countDown();
            }
          }
        }
      });

    consumerThread.set(thread);
    startMetricsLogThread();
    LOGGER.log(Level.INFO, "Consumer started for topics {0}", topics);
  }

  /// Pauses consumption from the topic. Any in-flight messages will continue processing, but no new
  /// messages will be consumed until [#resume()] is called.
  ///
  /// While paused the consumer thread keeps calling `poll()` with every partition paused: the
  /// polls fetch nothing but retain the group membership, so a long pause does not trigger a
  /// `max.poll.interval.ms` eviction and the resulting rebalance.
  ///
  /// This method is idempotent - calling it multiple times has no additional effect.
  public void pause() {
    if (health.requestPause(ConsumerHealthController.Source.MANUAL)) internalPause();
  }

  private void internalPause() {
    final var current = state.get();
    if (current != ConsumerState.RUNNING && current != ConsumerState.CREATED) return;
    if (state.compareAndSet(current, ConsumerState.PAUSED)) {
      commandQueue.offer(new ConsumerCommand.Pause());
    }
  }

  /// Drains the command queue on the consumer thread. Commands are the only mechanism by which
  /// off-thread callers can ask Kafka APIs (`pause` / `resume` / `commitSync`) or the
  /// [OffsetManager] to mutate state — serializing them here keeps those calls single-threaded
  /// without locks.
  ///
  /// Recognized commands:
  ///
  /// * `Pause` — `kafkaConsumer.pause(assignment)`
  /// * `Resume` — `kafkaConsumer.resume(assignment)`
  /// * `Close` — flips the consumer to `CLOSING`
  /// * `CommitOffsets` — invokes the command's offsets supplier (execution-time frontier, see
  ///   [CommitExecutor]), runs `kafkaConsumer.commitSync`, completes the outcome future
  ///
  /// Offset tracking + per-record marking do NOT go through this queue — they call `OffsetManager`
  /// directly from whichever thread finishes processing (the manager is already thread-safe).
  ///
  /// Commands are processed in submission order. Per-command exceptions are logged at ERROR and
  /// swallowed so a malformed command cannot halt subsequent ones.
  void processCommands() {
    ConsumerCommand command;

    while ((command = commandQueue.poll()) != null) {
      try {
        switch (command) {
          case ConsumerCommand.Pause _ -> {
            kafkaConsumer.pause(kafkaConsumer.assignment());
            LOGGER.log(Level.INFO, "Consumer paused for topics {0}", topics);
          }
          case ConsumerCommand.Resume _ -> {
            kafkaConsumer.resume(kafkaConsumer.assignment());
            LOGGER.log(Level.INFO, "Consumer resumed for topics {0}", topics);
          }
          case ConsumerCommand.Close _ -> {
            state.set(ConsumerState.CLOSING);
            LOGGER.log(Level.INFO, "Consumer shutdown initiated for topics {0}", topics);
          }
          case ConsumerCommand.CommitOffsets cmd -> {
            // The supplier reads the submitter's commit frontier NOW, on this thread — after any
            // rebalance callback that already ran inside poll(). A request queued before a revoke
            // therefore never sees the revoked partitions (their ledger state is cleared), which
            // is what makes queued commits rebalance-safe without rewriting queue payloads.
            var success = false;
            try {
              final var offsets = cmd.offsets().get();
              if (!offsets.isEmpty()) kafkaConsumer.commitSync(offsets);
              success = true;
            } catch (final Exception e) {
              LOGGER.log(Level.WARNING, "Failed to commit offsets", e);
            }
            cmd.outcome().complete(success);
          }
        }
      } catch (final Exception e) {
        LOGGER.log(Level.ERROR, "Error processing consumer command: {0}", command, e);
      }
    }
  }

  /// Resumes consumption from the topic after being paused.
  ///
  /// This method is idempotent - calling it multiple times has no additional effect.
  ///
  /// @throws IllegalStateException if the consumer has been closed
  public void resume() {
    if (state.get() == ConsumerState.CLOSED) throw new IllegalStateException("Cannot resume a closed consumer");
    if (health.releasePause(ConsumerHealthController.Source.MANUAL)) internalResume();
  }

  private void internalResume() {
    if (!state.compareAndSet(ConsumerState.PAUSED, ConsumerState.RUNNING)) return;
    commandQueue.offer(new ConsumerCommand.Resume());
    final var thread = consumerThread.get();
    if (thread != null) LockSupport.unpark(thread);
  }

  /// Returns whether the consumer is currently paused.
  ///
  /// @return `true` if the consumer is paused, `false` otherwise
  public boolean isPaused() {
    return state.get() == ConsumerState.PAUSED;
  }

  /// The consumer's counters now: records received, processed, filtered and failed, in total
  /// and per topic, plus in-flight records, backpressure and circuit breaker figures.
  ///
  /// @return a snapshot of the counters
  public MetricsSnapshot metrics() {
    return counters.snapshot(backpressureLoad());
  }

  private MetricsSource metricsSource() {
    return new MetricsSource() {
      @Override
      public String consumerGroup() {
        return consumerGroup;
      }

      @Override
      public MetricsSnapshot metrics() {
        return KPipeConsumer.this.metrics();
      }

      @Override
      public HealthSnapshot health() {
        return KPipeConsumer.this.health();
      }
    };
  }

  /// Snapshot of the top `n` keys by current queue depth in the underlying dispatcher,
  /// ordered deepest-first. Returns an empty list for SEQUENTIAL and PARALLEL modes (no
  /// per-key structure); only [ProcessingMode#KEY_ORDERED] populates this. Intended for
  /// ad-hoc diagnostics — heap-dump replacement, REPL inspection, JMX panels — not for
  /// continuous metric emission, since per-key cardinality is unbounded.
  ///
  /// @param n maximum number of entries to return (must be positive)
  /// @return ordered list of `(key, queueDepth)` entries; never null, may be empty
  public List<Map.Entry<byte[], Integer>> topKeyQueueDepths(final int n) {
    return dispatcher.topKeyQueueDepths(n);
  }

  /// Returns the rebalance listener for this consumer.
  ///
  /// @return the rebalance listener for this consumer
  public ConsumerRebalanceListener getRebalanceListener() {
    return rebalanceListener;
  }

  /// Returns whether the consumer is running.
  ///
  /// @return `true` if the consumer is running, `false` if it has been closed or not
  ///     started
  public boolean isRunning() {
    final var s = state.get();
    return s == ConsumerState.RUNNING || s == ConsumerState.PAUSED;
  }

  /// Whether `close()` has begun or finished. Not the same as `!isRunning()`, which is also true
  /// before `start()`.
  private boolean closingStarted() {
    final var s = state.get();
    return s == ConsumerState.CLOSING || s == ConsumerState.CLOSED;
  }

  /// Point-in-time health snapshot: running/paused state, active pause sources, circuit-breaker
  /// state, and in-flight count. This is the source of truth for liveness probes — see
  /// [HealthSnapshot#healthy()] for why `isRunning()` alone is the wrong probe signal (a
  /// breaker-tripped consumer is alive but deliberately not consuming).
  ///
  /// @return an immutable snapshot of this consumer's health
  public HealthSnapshot health() {
    final var sources = health.currentSources().stream().map(Enum::name).collect(Collectors.toUnmodifiableSet());
    return new HealthSnapshot(
      isRunning(),
      health.isPaused(),
      sources,
      health.circuitBreakerState(),
      backpressureLoad()
    );
  }

  /// Atomically transitions from any active state (RUNNING or PAUSED) to CLOSING.
  /// Uses a single-read CAS to avoid the double-CAS window.
  ///
  /// @return true if the transition succeeded, false if the state was not active
  private boolean transitionToClosing() {
    return tryTransitionToClosing(state);
  }

  /// The single-read compare-and-set that backs [#transitionToClosing], lifted to a static method
  /// over the supplied reference so the exact discipline (read once into a local, verify the state
  /// is active, then CAS to CLOSING) can be exercised under Fray without constructing a full
  /// consumer. Two threads racing this must yield exactly one winner; the loser sees CLOSING and
  /// returns false. Package-private for that test only.
  ///
  /// @param state the lifecycle state reference to transition
  /// @return true if this call performed the transition, false if the state was not active or
  ///         another thread won the CAS
  static boolean tryTransitionToClosing(final AtomicReference<ConsumerState> state) {
    final var current = state.get();
    if (current != ConsumerState.RUNNING && current != ConsumerState.PAUSED) return false;
    return state.compareAndSet(current, ConsumerState.CLOSING);
  }

  /// Closes this consumer, stopping message consumption and processing.
  ///
  /// Three terminal paths converge on the same final state (`CLOSED`, latch released, all
  /// resources freed):
  ///
  /// 1. **Never-started fast path.** CREATED → CLOSED via [#closeNeverStarted]: release every
  ///    constructor-created resource, close the Kafka consumer directly, count down the latch.
  /// 2. **Normal shutdown.** Transition to CLOSING, then [#initiateShutdown] (signal the offset
  ///    manager, stop reporter, queue Close, signal dispatcher, drain in-flight), then
  ///    [#waitForConsumerThreadToJoin] (wakeup + unpark + join). If the thread joined cleanly,
  ///    [#finalizeAfterThreadJoined] runs the (idempotent) release + state=CLOSED + countDown as
  ///    a safety net — the consumer thread's own finally has already done the work in the common
  ///    case.
  /// 3. **Join-timeout escape.** The consumer thread is still alive after
  ///    `threadTerminationTimeout` (e.g. SEQUENTIAL mode stuck in a hung pipeline). Interrupt
  ///    and return — tearing down resources here would race a live thread that still holds the
  ///    offset manager / producer. The thread's own finally handles the final transition when
  ///    it eventually exits.
  ///
  /// The consumer thread's own finally (in [#start]) mirrors path 2's teardown when it
  /// self-terminates (uncaught throw, internal error). All terminal paths CAS-guard
  /// [#releaseConstructedResources] so resources are freed exactly once regardless of which
  /// path runs first.
  ///
  /// Idempotent — calling it multiple times has no additional effect.
  @Override
  public void close() {
    if (state.compareAndSet(ConsumerState.CREATED, ConsumerState.CLOSED)) {
      closeNeverStarted();
      return;
    }
    if (!transitionToClosing()) return;

    initiateShutdown();
    if (!waitForConsumerThreadToJoin()) return;
    finalizeAfterThreadJoined();
  }

  /// Path 1 of [#close]: the consumer was built but never started, so the consumer thread that
  /// normally closes `kafkaConsumer` in its finally never ran. Release every constructor-created
  /// resource, close the Kafka consumer directly, and unblock [#awaitShutdown] callers.
  private void closeNeverStarted() {
    releaseConstructedResources();
    closeKafkaConsumerQuietly();
    shutdownLatch.countDown();
  }

  /// Signal the offset manager, stop the metrics log thread, ask the consumer loop to halt, and let
  /// any in-flight records drain before we start waking and joining the consumer thread.
  ///
  /// No pause command needed: CLOSING already halts the poll loop ([#isRunning] is false), and
  /// the subsequent `wakeup`/`unpark` in [#waitForConsumerThreadToJoin] breaks any in-progress
  /// poll. The Close command exists for logging + symmetry; the actual halt signal is the state
  /// transition done by [#transitionToClosing]. The dispatcher signal is a no-op for
  /// SEQUENTIAL/PARALLEL; KEY_ORDERED uses it to break out of its saturation yield-loop so the
  /// consumer thread can drop its pending count and let `close()` proceed promptly instead of
  /// spinning past `threadTerminationTimeout`.
  private void initiateShutdown() {
    if (offsetManager != null) offsetManager.signalClosing();
    stopMetricsLogThread();
    commandQueue.offer(new ConsumerCommand.Close());
    dispatcher.signalShutdown();
    if (waitForMessagesTimeout.toMillis() > 0 && dispatcher.drainableCount() > 0) {
      LOGGER.log(Level.INFO, "Waiting for {0} in-flight messages to complete", dispatcher.drainableCount());
      waitForInFlightDrain(waitForMessagesTimeout);
    }
  }

  /// Wakes the consumer thread and joins it within `threadTerminationTimeout`.
  ///
  /// @return `true` if the thread terminated (or was never registered, which can happen if
  ///     `close()` races with the tail of `start()` before `consumerThread.set(...)`); `false`
  ///     if the join timed out and the thread is still alive — caller must NOT tear down
  ///     resources in that case (use-after-close hazard).
  private boolean waitForConsumerThreadToJoin() {
    if (kafkaConsumer != null) {
      try {
        kafkaConsumer.wakeup();
      } catch (final Exception e) {
        LOGGER.log(Level.WARNING, "Error during wakeup", e);
      }
    }

    final var thread = consumerThread.get();
    if (thread != null) LockSupport.unpark(thread);

    if (thread != null && thread.isAlive()) {
      try {
        thread.join(threadTerminationTimeout.toMillis());
      } catch (final InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }

    if (thread != null && thread.isAlive()) {
      LOGGER.log(
        Level.WARNING,
        "Consumer thread did not terminate within {0}ms; interrupting and leaving teardown to its finally block",
        threadTerminationTimeout.toMillis()
      );
      thread.interrupt();
      return false;
    }
    return true;
  }

  /// Idempotent finalizer for path 2 of [#close]. On the normal path the consumer thread's own
  /// finally has already run [#releaseConstructedResources] (which closes `kafkaConsumer` and
  /// removes the shutdown hook) + state=CLOSED + countDown; this is the safety net for the
  /// `consumerThread.get() == null` race (close() called between the VT spawn and the
  /// `consumerThread.set(...)` line in `start()`), where the VT may exit before either side has
  /// finalized. CAS guards inside `releaseConstructedResources` and on the latch keep the
  /// double-invoke safe.
  private void finalizeAfterThreadJoined() {
    releaseConstructedResources();
    state.set(ConsumerState.CLOSED);
    shutdownLatch.countDown();
  }

  /// Closes every resource the constructor created EXCEPT `kafkaConsumer`. Invoked from three
  /// paths, CAS-guarded so it runs exactly once: (1) the normal `close()` path after the
  /// consumer thread joins; (2) the never-started fast path; (3) the consumer thread's own
  /// finally when it self-terminates (uncaught throwable / interruption) without an external
  /// `close()` — in that case it sets state=CLOSED, which would make a later `close()` a no-op,
  /// so this is the only chance to release these resources.
  ///
  /// `kafkaConsumer` is excluded: on paths (1) and (3) the consumer thread's finally closes it,
  /// and on path (2) the fast path closes it via [#closeKafkaConsumerQuietly]. Closing a
  /// KafkaConsumer from two threads would throw ConcurrentModificationException. Order matters —
  /// dispatcher → batch wrappers → offset manager → producer → metrics export — so batch
  /// buffers flush (marking their offsets) while the offset manager is still alive, and the
  /// export's final read holds every count the shutdown completed. Critically, all three
  /// callers run this BEFORE closing `kafkaConsumer`: `offsetManager.close()` does a final
  /// commitSync on the consumer, which would throw (and silently drop the last offsets) on an
  /// already-closed one.
  private void releaseConstructedResources() {
    if (!resourcesReleased.compareAndSet(false, true)) return; // already released by another path
    counters.stopClocks();
    counters.lagSampled(Map.of());
    // Every step is best-effort: the CAS above already fired, so a throw here would leak the
    // remaining resources (no path retries). Catch + log each so teardown always runs to the end.
    try {
      dispatcher.close();
    } catch (final Exception e) {
      LOGGER.log(Level.WARNING, "Error closing dispatcher during shutdown", e);
    }
    for (final var wrapper : batchWrappers.values()) {
      try {
        wrapper.close();
      } catch (final Exception e) {
        LOGGER.log(Level.WARNING, "Error draining batch buffer during shutdown", e);
      }
    }
    try {
      health.shutdown();
    } catch (final Exception e) {
      LOGGER.log(Level.WARNING, "Error cancelling the circuit-breaker probe timer during shutdown", e);
    }
    if (scheduler != null) scheduler.shutdownNow();
    // A pending interrupt, from close() giving up on the join or restored by a stuck record or an
    // interrupted batch flush, has already broken the wait it was meant for. The final offset
    // commit is bounded by its own timeout, but Kafka refuses to run it on an interrupted thread,
    // so clear the flag for it and restore it once teardown ends. The batch flushes above keep the
    // flag: a sink can block without bound, and an interrupt is the only thing that ends it.
    final var interrupted = Thread.interrupted();
    try {
      if (offsetManager != null) {
        try {
          offsetManager.close();
        } catch (final Exception e) {
          LOGGER.log(Level.WARNING, "Error closing offset manager", e);
        }
      }
      if (kpipeProducer != null) {
        try {
          kpipeProducer.close(ConsumerDefaults.PRODUCER_CLOSE);
        } catch (final Exception e) {
          LOGGER.log(Level.WARNING, "Error closing producer during shutdown", e);
        }
      }
      // Close the export and write the final metrics line after every flush and DLQ write
      // above, so both read the consumer's final counts.
      exporter.close();
      if (metricsLogThread != null) logMetrics();
      // Deregister the JVM shutdown hook last. It's registered in the ctor (when
      // useShutdownHook=true), so it exists even for a never-started consumer and a
      // self-terminating consumer thread. Doing it here — inside the CAS-guarded release — means
      // every terminal path removes it; otherwise a self-terminated thread (which sets
      // state=CLOSED, making a later close() a no-op at transitionToClosing) would leave the
      // hook registered for the JVM's lifetime, pinning this consumer in memory.
      tryRemoveShutdownHook();
    } finally {
      if (interrupted) Thread.currentThread().interrupt();
    }
  }

  /// Closes `kafkaConsumer`, swallowing+logging any error. Used only on the never-started fast
  /// path, where the consumer thread (the normal owner of this close) never ran.
  private void closeKafkaConsumerQuietly() {
    if (kafkaConsumer == null) return;
    try {
      kafkaConsumer.close(CloseOptions.timeout(ConsumerDefaults.KAFKA_CLIENT_CLOSE));
    } catch (final Exception e) {
      LOGGER.log(Level.WARNING, "Error closing Kafka consumer", e);
    }
  }

  /// Removes the JVM shutdown hook installed in the constructor when `useShutdownHook=true`.
  /// No-op when no hook was installed or when the JVM is already shutting down (in which case
  /// `removeShutdownHook` would throw `IllegalStateException` — caught and ignored, the hook is
  /// running anyway).
  private void tryRemoveShutdownHook() {
    final var hook = shutdownHookThread;
    if (hook == null) return;
    try {
      Runtime.getRuntime().removeShutdownHook(hook);
    } catch (final IllegalStateException e) {
      // JVM already shutting down — the hook is currently executing, which is exactly the path
      // that called us. Leave it alone.
    } finally {
      shutdownHookThread = null;
    }
  }

  /// Processes a batch of Kafka records by dispatching each to the configured [Dispatcher].
  /// The dispatcher chooses the per-record execution thread according to its [ProcessingMode]:
  ///
  /// - `SequentialDispatcher` runs each record inline on the consumer thread.
  /// - `ParallelDispatcher` submits each record to its virtual-thread executor.
  /// - `KeyOrderedDispatcher` enqueues records onto a per-key serial queue + virtual thread.
  ///
  /// The dispatcher also owns the in-flight counter (for non-sequential modes) and feeds
  /// [#backpressureLoad] via `dispatcher.drainableCount()`.
  ///
  /// @param records the batch of records to process
  protected void processRecords(final ConsumerRecords<byte[], byte[]> records) {
    for (final var record : records) {
      // SEQUENTIAL runs each record on this thread, so once close() has begun, finishing the rest
      // of the batch only delays shutdown past its join budget. A record never tracked is never
      // committed past: the frontier stops at the last one processed, and the rest are fetched
      // again on restart.
      if (processingMode == ProcessingMode.SEQUENTIAL && closingStarted()) return;
      if (offsetManager != null) offsetManager.trackOffset(record);
      dispatcher.dispatch(record, () -> processRecord(record), this::wakeIfBackpressureDrained);
    }
  }

  /// Surfaces a rejection from `ParallelDispatcher`'s executor (typically during shutdown)
  /// back to the processing engine's error path. Kept as an instance method so the dispatcher
  /// can be constructed (with a method reference) before the engine field is assigned.
  private void handleParallelRejection(
    final ConsumerRecord<byte[], byte[]> record,
    final RejectedExecutionException e
  ) {
    recordProcessor.handleParallelRejection(record, e);
  }

  /// Runs whenever the in-flight load drops: after every record finishes, and after every batch
  /// flush, on whichever thread did the work. A batch route's records finish as soon as they are
  /// buffered, so for those the flush is what drains the load. Unparks the consumer thread once an
  /// in-flight backpressure hold has drained to the low watermark, which ends the wait in
  /// [#pollAfterBackpressureWait] so the consumer resumes without idling out a poll timeout. Calls
  /// above the low watermark leave the thread parked, because waking it would only re-check a hold
  /// that cannot release yet. The unpark is a permit, so one that lands before the consumer thread
  /// parks still ends that park.
  private void wakeIfBackpressureDrained() {
    if (health.inFlightBackpressureDrained()) {
      final var thread = consumerThread.get();
      if (thread != null) LockSupport.unpark(thread);
    }
  }

  /// Waits out an in-flight backpressure hold, then polls without blocking.
  ///
  /// A paused `poll(timeout)` cannot be cut short by a record completion: the client waits in its
  /// network selector, which `LockSupport.unpark` does not reach, so the consumer would sit idle
  /// for the whole poll timeout after the in-flight count had already drained. Parking instead
  /// lets [#wakeIfBackpressureDrained] end the wait the moment the hold can release. The park is
  /// still bounded by `pollTimeout`, and the zero-timeout poll after it keeps the group membership
  /// alive and runs any rebalance callbacks.
  ///
  /// A zero-timeout poll advances a classic-protocol join or sync by one step, where a blocking
  /// poll completes it within one call. A rebalance that starts during the hold therefore takes up
  /// to a few extra poll timeouts to finish while the hold lasts.
  ///
  /// The drained check before parking pairs with the unpark in [#wakeIfBackpressureDrained]: the
  /// hold is already published when it runs, so a completion that drains the count after this
  /// read observes the hold and leaves a permit that ends the park at once.
  ///
  /// @return the records fetched, or `null` when the poll was woken or failed
  private ConsumerRecords<byte[], byte[]> pollAfterBackpressureWait() {
    if (!health.inFlightBackpressureDrained()) LockSupport.parkNanos(pollTimeout.toNanos());
    return pollRecords(Duration.ZERO);
  }

  /// Processes a single Kafka consumer record by delegating to the [RecordProcessor] engine:
  /// the full deserialize → operators → sink → mark/DLQ unit, including retries per
  /// `withRetry(maxRetries, backoff)` and tracing-span handling. Runs in the current
  /// virtual thread. Kept on the consumer (and `protected`) as the per-record entry point for
  /// the dispatcher and for tests that drive a single record without a poll loop.
  ///
  /// @param record the Kafka consumer record to process
  protected void processRecord(final ConsumerRecord<byte[], byte[]> record) {
    recordProcessor.process(record);
  }

  /// Returns the load the in-flight backpressure strategy reads: `dispatcher.drainableCount()`
  /// plus the sum of records buffered across all configured batch wrappers.
  private long backpressureLoad() {
    var total = dispatcher.drainableCount();
    for (final var wrapper : batchWrappers.values()) total += wrapper.bufferedCount();
    return total;
  }

  /// Records each topic's lag from the client's fetch positions; no broker call. Runs on the
  /// consumer thread, the only one allowed to touch the Kafka consumer.
  private void sampleLag() {
    try {
      final var lag = new HashMap<String, Long>();
      for (final var partition : kafkaConsumer.assignment()) {
        final var partitionLag = kafkaConsumer.currentLag(partition);
        if (partitionLag.isPresent()) lag.merge(partition.topic(), partitionLag.getAsLong(), Long::sum);
      }
      counters.lagSampled(lag);
    } catch (final RuntimeException e) {
      LOGGER.log(Level.DEBUG, "Could not read consumer lag", e);
    }
  }

  private ConsumerRecords<byte[], byte[]> pollRecords(final Duration timeout) {
    try {
      return kafkaConsumer.poll(timeout);
    } catch (final WakeupException e) {
      // Expected during shutdown, no need to log
      return null;
    } catch (final InterruptException e) {
      // Propagate interruption
      Thread.currentThread().interrupt();
      return null;
    } catch (final Exception e) {
      // Only log if we're not shutting down
      if (isRunning()) LOGGER.log(Level.WARNING, "Error during Kafka poll operation", e);
      return null;
    }
  }
}
