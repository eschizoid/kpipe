package io.github.eschizoid.kpipe.consumer;

import io.github.eschizoid.kpipe.consumer.config.AppConfig;
import io.github.eschizoid.kpipe.metrics.ConsumerMetricKeys;
import io.github.eschizoid.kpipe.metrics.ConsumerMetrics;
import io.github.eschizoid.kpipe.metrics.KPipeMetricsReporter;
import io.github.eschizoid.kpipe.producer.KPipeProducer;
import io.github.eschizoid.kpipe.producer.tracing.Tracer;
import io.github.eschizoid.kpipe.registry.MessagePipeline;
import io.github.eschizoid.kpipe.registry.Result;
import io.github.eschizoid.kpipe.sink.BatchPolicy;
import io.github.eschizoid.kpipe.sink.BatchSink;
import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.nio.channels.ClosedByInterruptException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

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
/// When using an KafkaOffsetManager, auto-commit is automatically disabled since offset commits are
/// managed explicitly.
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

  // Aliases of the shared public key set (ConsumerMetricKeys) — the single source of truth also
  // read by ConsumerMetricsReporter and the kpipe-test quiescence check. Never re-declare the
  // literals here; aliasing keeps every call site short while making drift impossible.
  private static final String METRIC_MESSAGES_RECEIVED = ConsumerMetricKeys.MESSAGES_RECEIVED;
  private static final String METRIC_MESSAGES_PROCESSED = ConsumerMetricKeys.MESSAGES_PROCESSED;
  private static final String METRIC_PROCESSING_ERRORS = ConsumerMetricKeys.PROCESSING_ERRORS;
  private static final String METRIC_PROCESSING_DURATION_TOTAL_MS = ConsumerMetricKeys.PROCESSING_DURATION_TOTAL_MS;
  private static final String METRIC_RETRIES = ConsumerMetricKeys.RETRIES;
  static final String METRIC_BACKPRESSURE_PAUSE_COUNT = ConsumerMetricKeys.BACKPRESSURE_PAUSE_COUNT;
  static final String METRIC_BACKPRESSURE_TIME_MS = ConsumerMetricKeys.BACKPRESSURE_TIME_MS;
  static final String METRIC_CIRCUIT_BREAKER_TRIPS = ConsumerMetricKeys.CIRCUIT_BREAKER_TRIPS;
  static final String METRIC_CIRCUIT_BREAKER_TIME_OPEN_MS = ConsumerMetricKeys.CIRCUIT_BREAKER_TIME_OPEN_MS;
  private static final String METRIC_DLQ_SENT = ConsumerMetricKeys.DLQ_SENT;
  private static final String METRIC_DLQ_FAILED = ConsumerMetricKeys.DLQ_FAILED;

  private final Queue<ConsumerCommand> commandQueue;
  private final Consumer<byte[], byte[]> kafkaConsumer;
  private final Set<String> topics;
  private final Map<String, MessagePipeline<?>> pipelines;
  private final Duration pollTimeout;
  private final AtomicReference<Thread> consumerThread = new AtomicReference<>();
  private final Duration waitForMessagesTimeout;
  private final Duration threadTerminationTimeout;
  private final OffsetManager offsetManager;
  private final ConsumerRebalanceListener rebalanceListener;
  private final ErrorHandler errorHandler;
  private final int maxRetries;
  private final Duration retryBackoff;
  private final AtomicReference<ConsumerState> state = new AtomicReference<>(ConsumerState.CREATED);
  /// Guards [#releaseConstructedResources] so it runs exactly once no matter which path ends
  /// the consumer: external `close()`, the never-started fast path, or the consumer thread
  /// terminating on its own (uncaught exception / interruption). Without this, a self-
  /// terminating consumer thread would set state=CLOSED and a later `close()` would short-
  /// circuit, leaking the dispatcher / scheduler / offset manager / DLQ producer / batch
  /// wrappers for the JVM's lifetime.
  private final AtomicBoolean resourcesReleased = new AtomicBoolean(false);
  private final ProcessingMode processingMode;
  private final Map<String, AtomicLong> metrics = new ConcurrentHashMap<>();
  private final ConsumerMetrics otelMetrics;
  private final Tracer tracer;
  /// Owns the per-mode dispatch strategy and the in-flight counter for non-sequential modes.
  /// Constructed once in the consumer constructor from [#processingMode]; closed in `close()`
  /// before `offsetManager.close()` so any in-flight records get their offsets marked before
  /// the manager flushes its final commit.
  private final Dispatcher dispatcher;
  /// Composes pause arbitration + backpressure decision + circuit-breaker state machine. The
  /// underlying decision modules ([BackpressureController], [CircuitBreakerController]) remain
  /// public + testable on their own; this controller owns the side-effect choreography and
  /// dispatches pause transitions through a PauseLifecycleHook that points back at `internalPause` /
  /// `internalResume`, and metric events through a HealthMetricsObserver bound to the counters.
  private final ConsumerHealthController health;

  private final String deadLetterTopic;
  private final KPipeProducer<byte[], byte[]> kpipeProducer;

  private final Map<String, BatchPipelineWrapper<?>> batchWrappers;

  // ── Folded-in lifecycle host concerns (former KPipeRunner) ──
  /// Released by `close()` so callers blocked in [#awaitShutdown(Duration)] unblock when the
  /// consumer reaches CLOSED. Initialised once so the latch is stable across reconfiguration.
  private final CountDownLatch shutdownLatch = new CountDownLatch(1);
  /// Periodic metrics-reporter thread. Started at the end of `start()` if any reporters are
  /// configured; interrupted at the start of `close()`.
  private volatile Thread metricsReporterThread;
  private final List<KPipeMetricsReporter> metricsReporters;
  private final Duration metricsReporterInterval;
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
      if (builder.pipelinesPerTopic != null) {
        this.pipelines = builder.pipelinesPerTopic;
      } else if (builder.pipeline != null) {
        final var map = new LinkedHashMap<String, MessagePipeline<?>>(builder.topics.size());
        for (final var t : builder.topics) if (!builder.batchSpecs.containsKey(t)) map.put(t, builder.pipeline);
        this.pipelines = Map.copyOf(map);
      } else {
        this.pipelines = Map.of();
      }
      this.pollTimeout = Objects.requireNonNull(builder.pollTimeout);
      this.errorHandler = builder.errorHandler;
      this.maxRetries = builder.maxRetries;
      this.retryBackoff = builder.retryBackoff;
      this.processingMode = builder.processingMode;
      this.waitForMessagesTimeout = builder.waitForMessagesTimeout;
      this.threadTerminationTimeout = builder.threadTerminationTimeout;
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
          : BackpressureController.inFlightStrategy(this::totalInFlight)
      );

      this.tracer = builder.tracer != null ? builder.tracer : Tracer.noop();

      this.deadLetterTopic = builder.deadLetterTopic;
      this.kpipeProducer =
        builder.kpipeProducer != null
          ? builder.kpipeProducer
          : this.deadLetterTopic != null
            ? KPipeProducer.<byte[], byte[]>builder().withProperties(builder.kafkaProps).withTracer(this.tracer).build()
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

      if (!builder.batchSpecs.isEmpty()) {
        final var wrappers = new LinkedHashMap<String, BatchPipelineWrapper<?>>(builder.batchSpecs.size());
        for (final var entry : builder.batchSpecs.entrySet()) {
          wrappers.put(entry.getKey(), createBatchWrapper(entry.getValue()));
        }
        this.batchWrappers = Map.copyOf(wrappers);
      } else {
        this.batchWrappers = Map.of();
      }

      this.metricsReporters = List.copyOf(builder.metricsReporters);
      this.metricsReporterInterval = builder.metricsReporterInterval;
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

      metrics.putAll(
        Map.ofEntries(
          Map.entry(METRIC_MESSAGES_RECEIVED, new AtomicLong(0)),
          Map.entry(METRIC_MESSAGES_PROCESSED, new AtomicLong(0)),
          Map.entry(METRIC_PROCESSING_ERRORS, new AtomicLong(0)),
          Map.entry(METRIC_PROCESSING_DURATION_TOTAL_MS, new AtomicLong(0)),
          Map.entry(METRIC_RETRIES, new AtomicLong(0)),
          Map.entry(METRIC_BACKPRESSURE_PAUSE_COUNT, new AtomicLong(0)),
          Map.entry(METRIC_BACKPRESSURE_TIME_MS, new AtomicLong(0)),
          Map.entry(METRIC_CIRCUIT_BREAKER_TRIPS, new AtomicLong(0)),
          Map.entry(METRIC_CIRCUIT_BREAKER_TIME_OPEN_MS, new AtomicLong(0)),
          Map.entry(METRIC_DLQ_SENT, new AtomicLong(0)),
          Map.entry(METRIC_DLQ_FAILED, new AtomicLong(0))
        )
      );

      // Guarded once at the source: a throwing user metrics implementation must never crash a
      // worker, abort a batch-flush callback loop, or alter an error-path outcome.
      this.otelMetrics = GuardedConsumerMetrics.guard(builder.consumerMetrics);

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
            metrics.get(METRIC_BACKPRESSURE_PAUSE_COUNT).incrementAndGet();
            otelMetrics.recordBackpressurePause();
          }

          @Override
          public void onBackpressureTimeMs(final long ms) {
            metrics.get(METRIC_BACKPRESSURE_TIME_MS).addAndGet(ms);
            otelMetrics.recordBackpressureTime(ms);
          }

          @Override
          public void onCircuitBreakerTrip() {
            metrics.get(METRIC_CIRCUIT_BREAKER_TRIPS).incrementAndGet();
            otelMetrics.recordCircuitBreakerTrip();
          }

          @Override
          public void onCircuitBreakerStateChange(final CircuitBreakerState state) {
            otelMetrics.recordCircuitBreakerStateChange(state);
          }

          @Override
          public void onCircuitBreakerTimeOpenMs(final long ms) {
            metrics.get(METRIC_CIRCUIT_BREAKER_TIME_OPEN_MS).addAndGet(ms);
            otelMetrics.recordCircuitBreakerTimeOpen(ms);
          }
        }
      );
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
        kpipeProducer.close();
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
        kafkaConsumer.close();
      } catch (final Exception e) {
        original.addSuppressed(e);
      }
    }
  }

  private <T> BatchPipelineWrapper<T> createBatchWrapper(final KPipeConsumerBuilder.BatchSpec<T> spec) {
    // Batch flushes call the OffsetManager directly rather than going through commandQueue. The
    // queue exists to serialize Kafka-consumer calls (pause/resume/commitSync) on the consumer
    // thread; OffsetManager.markOffsetProcessed is already thread-safe and avoiding the queue
    // means the shutdown drain works even after the consumer thread has exited.
    final var callbacks = new BatchPipelineWrapper.BatchCallbacks() {
      @Override
      public void markProcessed(final ConsumerRecord<byte[], byte[]> record) {
        metrics.get(METRIC_MESSAGES_PROCESSED).incrementAndGet();
        otelMetrics.recordMessageProcessed(record.topic());
        // Feed the circuit breaker / health window so batch outcomes count toward the failure
        // rate, exactly like the per-record path. Both branches must report, or the rolling
        // window would see only failures and trip spuriously.
        health.recordOutcome(true);
        if (offsetManager != null) offsetManager.markOffsetProcessed(record);
      }

      @Override
      public void onBatchFailure(final ConsumerRecord<byte[], byte[]> record, final Exception cause) {
        metrics.get(METRIC_PROCESSING_ERRORS).incrementAndGet();
        otelMetrics.recordProcessingError(record.topic());
        health.recordOutcome(false);
        LOGGER.log(Level.WARNING, () -> "Batch failure for record at offset " + record.offset(), cause);
        // LOCKSTEP: mirror of the per-record path's DLQ-or-mark block in handleProcessingError —
        // mark the offset only after a successful DLQ send; a failed send leaves it pending so
        // the record is reprocessed, never dropped. Deliberately duplicated (the paths differ on
        // span handling, retry counts, and circuit-breaker ordering — recordOutcome runs BEFORE
        // this block here, AFTER it on the per-record path). Any change here must be mirrored
        // there; DlqTerminalContractTest asserts both paths cell-for-cell and fails on drift.
        if (deadLetterTopic != null && kpipeProducer != null) {
          if (kpipeProducer.sendToDlq(deadLetterTopic, record, record.topic(), cause)) {
            metrics.get(METRIC_DLQ_SENT).incrementAndGet();
            if (offsetManager != null) offsetManager.markOffsetProcessed(record);
          } else {
            metrics.get(METRIC_DLQ_FAILED).incrementAndGet();
            LOGGER.log(
              Level.ERROR,
              () ->
                "DLQ delivery failed for batch record at offset " +
                record.offset() +
                "; offset NOT committed, record will be reprocessed on restart/rebalance"
            );
          }
        } else if (offsetManager != null) {
          offsetManager.markOffsetProcessed(record);
        }
        try {
          errorHandler.accept(new ProcessingError(record, cause, 0));
        } catch (final Exception ex) {
          LOGGER.log(
            Level.ERROR,
            "Error handler threw on batch failure at offset {0}: {1}",
            record.offset(),
            ex.getMessage(),
            ex
          );
        }
      }
    };
    return new BatchPipelineWrapper<>(spec.topic(), spec.pipeline(), spec.sink(), spec.policy(), scheduler, callbacks);
  }

  /// Blocks until the dispatcher's in-flight records finish processing, or `timeout` elapses, or
  /// the calling thread is interrupted. Replaces the former standalone
  /// `MessageTracker.waitForCompletion(...)`.
  ///
  /// Waits on `dispatcher.activeCount()` — records a worker is actively processing — NOT
  /// `totalInFlight()`. The latter also counts buffered batch records, which never flush during a
  /// drain (only on a size/age trigger or `BatchPipelineWrapper.close()` at teardown), so waiting
  /// on them would always burn the full `timeout`. Buffered batches are flushed + committed by the
  /// teardown that follows, so a `true` return here means "active processing is done, safe to tear
  /// down."
  ///
  /// @param timeout maximum time to wait
  /// @return `true` if active processing drained in time, else `false`
  public boolean waitForInFlightDrain(final Duration timeout) {
    if (dispatcher.activeCount() == 0) return true;
    final var deadline = System.nanoTime() + timeout.toNanos();
    final var pollNanos = Math.min(timeout.toNanos() / 10, Duration.ofMillis(500).toNanos());
    final var pollMs = Math.max(1, pollNanos / 1_000_000);
    try {
      while (System.nanoTime() < deadline) {
        if (dispatcher.activeCount() == 0) return true;
        //noinspection BusyWait — deadline-bounded drain poll, not a spin
        Thread.sleep(pollMs);
      }
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
      return false;
    }
    return dispatcher.activeCount() == 0;
  }

  /// Runs on the consumer thread, at the top of its `finally`, before any teardown. The poll
  /// loop exits the moment `state` becomes CLOSING, so without this the finally would tear the
  /// dispatcher + offset manager down while in-flight workers are still finishing. We keep
  /// pumping the command queue (so any pending `CommitOffsets` triggered by the scheduler land on
  /// the still-open Kafka consumer) and wait for the dispatcher's pending work to settle, bounded
  /// by `waitForMessagesTimeout`. Finishing workers mark their offsets directly on the
  /// thread-safe `OffsetManager`, so those don't need the command queue. A final
  /// `processCommands()` flushes anything the last workers enqueued. Only the consumer thread may
  /// touch the Kafka consumer, so this must run here, not on the close() thread.
  ///
  /// We wait on `dispatcher.activeCount()`, NOT `totalInFlight()`: the latter also counts
  /// buffered batch records, which don't enqueue commands and won't flush on their own (size-only
  /// / long-maxAge policies). They're flushed by each `BatchPipelineWrapper.close()` in
  /// `releaseConstructedResources()` right after this returns — so waiting on them here would just
  /// burn the whole timeout while the dispatcher is already idle.
  private void drainInFlightBeforeTeardown() {
    if (waitForMessagesTimeout.toMillis() > 0) {
      final var deadline = System.nanoTime() + waitForMessagesTimeout.toNanos();
      while (dispatcher.activeCount() > 0 && System.nanoTime() < deadline) {
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
    if (snapshot == ConsumerState.CLOSING) return awaitShutdown(timeout) && totalInFlight() == 0;
    pause();
    final var drained = waitForInFlightDrain(timeout);
    close();
    return drained;
  }

  /// Starts the metrics-reporter thread if any reporters were configured on the builder. Called
  /// from `start()`; no-op when the list is empty. The thread is a platform daemon so it cannot
  /// keep the JVM alive past the consumer.
  private void startMetricsReporterThread() {
    if (metricsReporters.isEmpty() || metricsReporterInterval.toMillis() <= 0) return;
    metricsReporterThread = Thread.ofPlatform()
      .name("kpipe-metrics-reporter")
      .daemon(true)
      .start(() -> {
        final var current = Thread.currentThread();
        while (state.get() != ConsumerState.CLOSED && !current.isInterrupted()) {
          for (final var reporter : metricsReporters) {
            try {
              reporter.reportMetrics();
            } catch (final Exception e) {
              LOGGER.log(Level.WARNING, "Metrics reporter threw", e);
            }
          }
          try {
            //noinspection BusyWait — fixed reporting-interval sleep, not a spin
            Thread.sleep(metricsReporterInterval.toMillis());
          } catch (final InterruptedException e) {
            current.interrupt();
            break;
          }
        }
      });
  }

  /// Interrupts and joins the metrics-reporter thread if it's running. Called from `close()`.
  private void stopMetricsReporterThread() {
    final var t = metricsReporterThread;
    if (t == null) return;
    t.interrupt();
    try {
      t.join(1_000);
    } catch (final InterruptedException e) {
      Thread.currentThread().interrupt();
    } finally {
      metricsReporterThread = null;
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
            if (isPaused()) {
              // Flush the Pause command queued by the transition above so
              // `kafkaConsumer.pause()` executes before the poll below, then defensively
              // re-pause the full assignment: pause state is per-partition and does not
              // survive a revoke/assign cycle, so a rebalance inside an earlier poll can
              // hand this consumer new, un-paused partitions.
              processCommands();
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
            final var records = pollRecords();
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
            try {
              kafkaConsumer.close();
              LOGGER.log(Level.INFO, "Consumer closed for topics {0}", topics);
            } catch (final Exception e) {
              LOGGER.log(Level.WARNING, "Error closing Kafka consumer", e);
            } finally {
              state.set(ConsumerState.CLOSED);
              shutdownLatch.countDown();
            }
          }
        }
      });

    consumerThread.set(thread);
    startMetricsReporterThread();
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
  /// * `CommitOffsets` — `kafkaConsumer.commitSync(offsets)` plus offset-manager notification
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
            try {
              kafkaConsumer.commitSync(cmd.offsets());
              if (offsetManager != null) offsetManager.notifyCommitComplete(cmd.commitId(), true);
            } catch (Exception e) {
              LOGGER.log(Level.WARNING, "Failed to commit offsets", e);
              if (offsetManager != null) offsetManager.notifyCommitComplete(cmd.commitId(), false);
            }
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

  /// Returns a snapshot of the current metrics collected by this consumer.
  ///
  /// Available metrics:
  ///
  /// * `messagesReceived` — records received from Kafka
  /// * `messagesProcessed` — records successfully processed
  /// * `processingErrors` — records that failed processing after all retries
  /// * `processingDurationTotalMs` — total wall-clock time spent inside the pipeline
  ///   (deserialize → operators → sink)
  /// * `retries` — retry attempts made for failed records
  /// * `backpressurePauseCount` / `backpressureTimeMs` — backpressure pause count and total ms held
  /// * `circuitBreakerTrips` / `circuitBreakerTimeOpenMs` — CB trip count and total ms in OPEN
  /// * `dlqSent` — records sent to the configured DLQ topic
  /// * `dlqFailed` — records whose DLQ send failed; the offset is held (reprocessed on
  ///   restart/rebalance), so a rising count signals a stalling DLQ
  /// * `inFlight` — current number of messages held by the consumer (live counter, not a counter
  /// delta).
  ///   Sums the dispatcher's pending count (records currently being processed or queued per-key)
  ///   AND any records buffered inside batch-sink wrappers awaiting size/age flush.
  ///
  /// @return an unmodifiable map of metric names to their current values
  public Map<String, Long> getMetrics() {
    final var snapshot = metrics
      .entrySet()
      .stream()
      .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().get(), (a, _) -> a, HashMap::new));
    snapshot.put(ConsumerMetricKeys.IN_FLIGHT, totalInFlight());
    return Collections.unmodifiableMap(snapshot);
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

  /// Atomically transitions from any active state (RUNNING or PAUSED) to CLOSING.
  /// Uses a single-read CAS to avoid the double-CAS window.
  ///
  /// @return true if the transition succeeded, false if the state was not active
  private boolean transitionToClosing() {
    return tryTransitionToClosing(state);
  }

  /// The single-read compare-and-set that backs [#transitionToClosing], lifted to a static method
  /// over the supplied reference so the exact discipline (read once into a local, verify the state
  /// is active, then CAS to CLOSING) can be exercised under jcstress without constructing a full
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
  /// 2. **Normal shutdown.** Transition to CLOSING, then [#initiateShutdown] (stop reporter,
  ///    queue Close, signal dispatcher, drain in-flight), then [#waitForConsumerThreadToJoin]
  ///    (wakeup + unpark + join). If the thread joined cleanly, [#finalizeAfterThreadJoined]
  ///    runs the (idempotent) release + state=CLOSED + countDown as a safety net — the consumer
  ///    thread's own finally has already done the work in the common case.
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

  /// Stop the metrics reporter, ask the consumer loop to halt, and let any in-flight records
  /// drain before we start waking and joining the consumer thread.
  ///
  /// No pause command needed: CLOSING already halts the poll loop ([#isRunning] is false), and
  /// the subsequent `wakeup`/`unpark` in [#waitForConsumerThreadToJoin] breaks any in-progress
  /// poll. The Close command exists for logging + symmetry; the actual halt signal is the state
  /// transition done by [#transitionToClosing]. The dispatcher signal is a no-op for
  /// SEQUENTIAL/PARALLEL; KEY_ORDERED uses it to break out of its saturation yield-loop so the
  /// consumer thread can drop its pending count and let `close()` proceed promptly instead of
  /// spinning past `threadTerminationTimeout`.
  private void initiateShutdown() {
    stopMetricsReporterThread();
    commandQueue.offer(new ConsumerCommand.Close());
    dispatcher.signalShutdown();
    if (waitForMessagesTimeout.toMillis() > 0 && dispatcher.activeCount() > 0) {
      LOGGER.log(Level.INFO, "Waiting for {0} in-flight messages to complete", dispatcher.activeCount());
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
  /// dispatcher → batch wrappers → offset manager → producer — so batch buffers flush (marking
  /// their offsets) while the offset manager is still alive. Critically, all three callers run
  /// this BEFORE closing `kafkaConsumer`: `offsetManager.close()` does a final commitSync on the
  /// consumer, which would throw (and silently drop the last offsets) on an already-closed one.
  private void releaseConstructedResources() {
    if (!resourcesReleased.compareAndSet(false, true)) return; // already released by another path
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
    if (offsetManager != null) {
      try {
        offsetManager.close();
      } catch (final Exception e) {
        LOGGER.log(Level.WARNING, "Error closing offset manager", e);
      }
    }
    if (kpipeProducer != null) {
      try {
        kpipeProducer.close();
      } catch (final Exception e) {
        LOGGER.log(Level.WARNING, "Error closing producer during shutdown", e);
      }
    }
    // Deregister the JVM shutdown hook last. It's registered in the ctor (when
    // useShutdownHook=true), so it exists even for a never-started consumer and a
    // self-terminating consumer thread. Doing it here — inside the CAS-guarded release — means
    // every terminal path removes it; otherwise a self-terminated thread (which sets
    // state=CLOSED, making a later close() a no-op at transitionToClosing) would leave the
    // hook registered for the JVM's lifetime, pinning this consumer in memory.
    tryRemoveShutdownHook();
  }

  /// Closes `kafkaConsumer`, swallowing+logging any error. Used only on the never-started fast
  /// path, where the consumer thread (the normal owner of this close) never ran.
  private void closeKafkaConsumerQuietly() {
    if (kafkaConsumer == null) return;
    try {
      kafkaConsumer.close();
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
  /// [#totalInFlight] via `dispatcher.activeCount()`.
  ///
  /// @param records the batch of records to process
  protected void processRecords(final ConsumerRecords<byte[], byte[]> records) {
    for (final var record : records) {
      if (offsetManager != null) offsetManager.trackOffset(record);
      dispatcher.dispatch(record, () -> processRecord(record), this::afterRecordComplete);
    }
  }

  /// Surfaces a rejection from `ParallelDispatcher`'s executor (typically during shutdown)
  /// back to the consumer's error path. Mirrors the prior inline behavior in `processRecords`.
  private void handleParallelRejection(
    final ConsumerRecord<byte[], byte[]> record,
    final RejectedExecutionException e
  ) {
    if (!isRunning()) return;
    LOGGER.log(Level.WARNING, "Task submission rejected during shutdown", e);
    metrics.get(METRIC_PROCESSING_ERRORS).incrementAndGet();
    otelMetrics.recordProcessingError(record.topic());
    try {
      errorHandler.accept(new ProcessingError(record, e, 0));
    } catch (final Exception ex) {
      LOGGER.log(
        Level.ERROR,
        () -> "Error handler threw while handling rejected task at offset " + record.offset(),
        ex
      );
    }
  }

  /// Runs after every record finishes (on whichever thread executed it). Unparks the consumer
  /// thread when backpressure is currently holding it — a record completion may have dropped
  /// the in-flight count below the low watermark. This is a best-effort latency nudge, not a
  /// liveness requirement: the paused loop no longer parks indefinitely (it keeps polling paused
  /// partitions on a `pollTimeout` cadence and re-evaluates backpressure at the top of every
  /// iteration), so the unpark only shortens the bounded `parkNanos` waits on the teardown drain
  /// path and is a no-op while the consumer thread is blocked inside `poll()`. Previously inlined
  /// into `processRecord`'s finally block; moved here so the dispatcher owns the post-record
  /// callback uniformly across modes.
  private void afterRecordComplete() {
    if (health.isHeldBy(ConsumerHealthController.Source.BACKPRESSURE)) {
      final var thread = consumerThread.get();
      if (thread != null) LockSupport.unpark(thread);
    }
  }

  /// Processes a single Kafka consumer record using the topic's configured pipeline. Runs in the
  /// current virtual thread; retries on exception according to `maxRetries` + `retryBackoff`. On
  /// success the per-record outcome feeds the circuit-breaker window; on retry-exhausted failure
  /// the record is routed to the DLQ (when configured) and the error handler is invoked.
  ///
  /// Metrics tracked during processing:
  ///
  /// * `messagesReceived` — incremented on entry
  /// * `messagesProcessed` — incremented on success
  /// * `processingDurationTotalMs` — incremented on success by the wall-clock duration
  /// * `retries` — incremented per retry attempt (not the initial attempt)
  /// * `processingErrors` — incremented when processing fails after all retries
  ///
  /// @param record the Kafka consumer record to process
  protected void processRecord(final ConsumerRecord<byte[], byte[]> record) {
    metrics.get(METRIC_MESSAGES_RECEIVED).incrementAndGet();
    otelMetrics.recordMessageReceived(record.topic());

    Tracer.SpanScope span;
    try {
      span = tracer.startConsumerSpan(record);
    } catch (final Exception traceEx) {
      LOGGER.log(Level.WARNING, "Tracer.startConsumerSpan threw", traceEx);
      span = Tracer.SpanScope.noop();
    }

    final long startTime = System.currentTimeMillis();
    try {
      final var result = tryProcessRecord(record, span);
      if (result) {
        final var durationMs = System.currentTimeMillis() - startTime;
        metrics.get(METRIC_MESSAGES_PROCESSED).incrementAndGet();
        metrics.get(METRIC_PROCESSING_DURATION_TOTAL_MS).addAndGet(durationMs);
        otelMetrics.recordMessageProcessed(record.topic());
        otelMetrics.recordProcessingDuration(record.topic(), durationMs);
      }
    } finally {
      try {
        span.close();
      } catch (final Exception traceEx) {
        LOGGER.log(Level.WARNING, "Tracer.SpanScope.close threw", traceEx);
      }
      // In-flight count + backpressure-unpark handled by the dispatcher's `onComplete`
      // callback (`afterRecordComplete`). See `processRecords`.
    }
  }

  private boolean tryProcessRecord(final ConsumerRecord<byte[], byte[]> record, final Tracer.SpanScope span) {
    final var batchWrapper = batchWrappers.get(record.topic());
    if (batchWrapper != null) return tryEnqueueBatchRecord(record, batchWrapper, span);
    final var pipeline = pipelines.get(record.topic());
    if (pipeline == null) {
      LOGGER.log(
        Level.WARNING,
        "No pipeline registered for topic {0}; dropping record at offset {1}",
        record.topic(),
        record.offset()
      );
      markOffsetProcessed(record);
      return false;
    }
    for (int attempt = 0; attempt <= maxRetries; attempt++) {
      if (attempt > 0) {
        metrics.get(METRIC_RETRIES).incrementAndGet();
        LOGGER.log(
          Level.DEBUG,
          "Retrying message at offset {0} (attempt {1} of {2})",
          record.offset(),
          attempt,
          maxRetries
        );
        try {
          Thread.sleep(retryBackoff.toMillis());
        } catch (final InterruptedException ie) {
          Thread.currentThread().interrupt();
          return false;
        }
      }

      try {
        driveSinkedPipeline(pipeline, record.value());
        markOffsetProcessed(record);
        health.recordOutcome(true);
        return true;
      } catch (final Exception e) {
        if (isInterruptionRelated(e)) {
          Thread.currentThread().interrupt();
          return false;
        }
        if (attempt == maxRetries) {
          handleProcessingError(record, e, attempt, span);
          return false;
        }
      }
    }
    return false;
  }

  /// Returns the total number of records currently being tracked for backpressure: whatever
  /// the per-mode dispatcher reports as pending plus the sum of records buffered across all
  /// configured batch wrappers. Without the buffered piece, a slow batch sink would let the
  /// buffer grow unbounded — the dispatcher decrements its counter the moment a record
  /// finishes `processRecord` (which for batch is "the record was buffered"), so the buffer
  /// would otherwise be invisible to the watermark check.
  private long totalInFlight() {
    var total = dispatcher.activeCount();
    for (final var wrapper : batchWrappers.values()) total += wrapper.bufferedCount();
    return total;
  }

  private <T> boolean tryEnqueueBatchRecord(
    final ConsumerRecord<byte[], byte[]> record,
    final BatchPipelineWrapper<T> wrapper,
    final Tracer.SpanScope span
  ) {
    try {
      final var pipeline = wrapper.pipeline();
      final var deserialized = pipeline.deserializeOrFail(record.value());
      switch (pipeline.process(deserialized)) {
        case Result.Passed<T> p -> {
          wrapper.enqueue(record, p.value());
          // Buffered: messagesProcessed will be incremented in the flush callback when the batch
          // is committed to the user sink.
          return false;
        }
        case Result.Filtered<T> _ -> {
          // Intentional filter — mark processed immediately; nothing to buffer.
          markOffsetProcessed(record);
          return true;
        }
        case Result.Failed<T> f -> throw rethrowResultCause(f.cause());
      }
    } catch (final Exception e) {
      if (isInterruptionRelated(e)) {
        Thread.currentThread().interrupt();
        return false;
      }
      handleProcessingError(record, e, 0, span);
      return false;
    }
  }

  /// Drive a pipeline (with erased element type) from raw bytes to its terminal sink. Throws if
  /// the pipeline reports `Failed` so the calling retry/error path handles it the same way it
  /// always did. Returns normally on `Passed` (after the sink runs) and on `Filtered`.
  private static <T> void driveSinkedPipeline(final MessagePipeline<T> pipeline, final byte[] data) {
    final var deserialized = pipeline.deserializeOrFail(data);
    switch (pipeline.process(deserialized)) {
      case Result.Passed<T> p -> {
        final var sink = pipeline.getSink();
        if (sink != null) sink.accept(p.value());
      }
      case Result.Filtered<T> _ -> {
        /* intentional filter — no sink invocation */
      }
      case Result.Failed<T> f -> throw rethrowResultCause(f.cause());
    }
  }

  /// Re-throw a captured `Result.Failed` cause as an unchecked exception so the retry/error path
  /// can catch it. Mirrors the legacy MessagePipeline byte-level entry point behavior — it just
  /// lives here now, where the catching happens, rather than buried in three duplicated unwrap
  /// blocks inside MessagePipeline.
  private static RuntimeException rethrowResultCause(final Throwable cause) {
    if (cause instanceof RuntimeException re) return re;
    if (cause instanceof Error err) throw err;
    return new RuntimeException(cause);
  }

  private void markOffsetProcessed(final ConsumerRecord<byte[], byte[]> record) {
    if (offsetManager != null) offsetManager.markOffsetProcessed(record);
  }

  private void handleProcessingError(
    final ConsumerRecord<byte[], byte[]> record,
    final Exception e,
    final int retryCount,
    final Tracer.SpanScope span
  ) {
    metrics.get(METRIC_PROCESSING_ERRORS).incrementAndGet();
    otelMetrics.recordProcessingError(record.topic());
    // Mark the span as errored. Guarded — a misbehaving tracer must never crash the consumer
    // thread, leak in-flight counts, or skip offset marking.
    try {
      span.recordException(e);
    } catch (final Exception traceEx) {
      LOGGER.log(Level.WARNING, "Tracer.SpanScope.recordException threw", traceEx);
    }
    LOGGER.log(
      Level.WARNING,
      () -> "Failed to process message at offset " + record.offset() + " after " + (retryCount + 1) + " attempt(s)",
      e
    );
    if (deadLetterTopic != null && kpipeProducer != null) {
      // The offset advances only once the record reaches a durable terminal state: either the sink
      // processed it (handled elsewhere) or it is safely parked in the DLQ. If the DLQ send fails
      // the record is in neither place, so leave the offset pending — the commit point holds and
      // the record is re-fetched (and the DLQ retried) on the next restart or partition
      // reassignment. A down DLQ stalls the commit point rather than silently dropping data
      // (the fetch position races ahead in-memory; this is a commit stall, not a pause).
      //
      // LOCKSTEP: this DLQ-or-mark block is deliberately duplicated in the batch wrapper's
      // onBatchFailure callback — the two paths differ on span handling, retry counts, and
      // circuit-breaker ordering (recordOutcome runs AFTER this block here, BEFORE it on the
      // batch path), so a shared helper would blur those. Any change here must be mirrored
      // there; DlqTerminalContractTest asserts both paths cell-for-cell and fails on drift.
      if (kpipeProducer.sendToDlq(deadLetterTopic, record, record.topic(), e)) {
        metrics.get(METRIC_DLQ_SENT).incrementAndGet();
        markOffsetProcessed(record);
      } else {
        metrics.get(METRIC_DLQ_FAILED).incrementAndGet();
        LOGGER.log(
          Level.ERROR,
          () ->
            "DLQ delivery failed for record at offset " +
            record.offset() +
            "; offset NOT committed, record will be reprocessed on restart/rebalance"
        );
      }
    } else {
      // No DLQ configured: the caller opted into log-and-advance. Mark processed and move on.
      markOffsetProcessed(record);
    }
    health.recordOutcome(false);
    try {
      errorHandler.accept(new ProcessingError(record, e, retryCount));
    } catch (final Exception ex) {
      LOGGER.log(
        Level.ERROR,
        "Error handler threw while handling failure at offset {0}: {1}",
        record.offset(),
        ex.getMessage(),
        ex
      );
    }
  }

  private static boolean isInterruptionRelated(final Throwable error) {
    for (Throwable current = error; current != null; current = current.getCause()) {
      if (current instanceof InterruptedException || current instanceof ClosedByInterruptException) return true;
    }
    return false;
  }

  private ConsumerRecords<byte[], byte[]> pollRecords() {
    try {
      return kafkaConsumer.poll(pollTimeout);
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
