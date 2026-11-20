package io.github.eschizoid.kpipe.consumer;

import io.github.eschizoid.kpipe.pipeline.NonRetryableException;
import io.github.eschizoid.kpipe.pipeline.Pipeline;
import io.github.eschizoid.kpipe.pipeline.Result;
import io.github.eschizoid.kpipe.producer.KPipeProducer;
import io.github.eschizoid.kpipe.record.RecordContext;
import io.github.eschizoid.kpipe.tracing.Tracer;
import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.time.Duration;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.BooleanSupplier;
import org.apache.kafka.clients.consumer.ConsumerRecord;

/// Per-record processing engine extracted from [KPipeConsumer]: everything that happens to one
/// record once a [Dispatcher] hands it a worker thread. Owns the full
/// deserialize → operators → sink → mark/DLQ unit including the retry loop, tracing-span
/// handling, the error/DLQ terminal paths, and the batch-route buffer plus its flush-callback
/// wiring. The consumer keeps lifecycle, threads, the poll loop, the command pump, dispatch,
/// and close orchestration.
///
/// The class holds no mutable state of its own — all fields are shared collaborators owned by
/// the consumer (the counters, the offset manager, the health controller, the DLQ producer)
/// — so [#process] is safe to invoke from any dispatcher worker thread.
///
/// Retry semantics are uniform across route shapes: both the regular per-record path and the
/// batch route drive their deserialize + operator step through the same attempt loop
/// ([#runWithRetries]). On the batch route only the pre-buffer step is retried; a successful
/// attempt buffers the value exactly once, and the batch sink flush itself is never retried
/// (flush failures take the `BatchResult` / DLQ path in the flush callbacks).
final class RecordProcessor {

  private static final Logger LOGGER = System.getLogger(RecordProcessor.class.getName());

  private final Map<String, Pipeline<?>> pipelines;
  private final Map<String, BatchPipelineWrapper<?>> batchWrappers;
  private final OffsetManager offsetManager;
  private final ConsumerHealthController health;
  private final GuardedMetricsExporter exporter;
  private final ConsumerCounters counters;
  private final KPipeConsumer.ErrorHandler attemptFailedObserver;
  private final KPipeConsumer.ErrorHandler errorHandler;
  private final String deadLetterTopic;
  private final KPipeProducer<byte[], byte[]> kpipeProducer;
  private final Tracer tracer;
  private final int maxRetries;
  private final Duration retryBackoff;
  /// Snapshot of the owning consumer's running state, used only by [#handleParallelRejection]
  /// to stay silent on rejections that are part of an orderly shutdown.
  private final BooleanSupplier consumerActive;
  /// Wakes the consumer thread when a batch flush may have drained an in-flight backpressure hold.
  private final Runnable loadReleased;

  /// Where a record ended up after its attempts.
  private enum Outcome {
    /// Passed every operator and was handed to the sink.
    PROCESSED,
    /// An operator filtered it.
    FILTERED,
    /// No pipeline is routed for its topic, so it was dropped.
    UNROUTED,
    /// Not terminal here: buffered for a batch flush, failed and handed to the error path, or
    /// abandoned on an interruption.
    PENDING,
  }

  /// One deserialize + process attempt for a record; throws to request another attempt from the
  /// surrounding retry loop.
  @FunctionalInterface
  private interface RecordAttempt {
    Outcome run(int attempt) throws Exception;
  }

  RecordProcessor(
    final Map<String, Pipeline<?>> pipelines,
    final Collection<KPipeConsumerBuilder.BatchSpec<?>> batchSpecs,
    final ScheduledExecutorService scheduler,
    final OffsetManager offsetManager,
    final ConsumerHealthController health,
    final GuardedMetricsExporter exporter,
    final ConsumerCounters counters,
    final KPipeConsumer.ErrorHandler attemptFailedObserver,
    final KPipeConsumer.ErrorHandler errorHandler,
    final String deadLetterTopic,
    final KPipeProducer<byte[], byte[]> kpipeProducer,
    final Tracer tracer,
    final int maxRetries,
    final Duration retryBackoff,
    final BooleanSupplier consumerActive,
    final Runnable loadReleased
  ) {
    this.pipelines = pipelines;
    this.offsetManager = offsetManager;
    this.health = health;
    this.exporter = exporter;
    this.counters = counters;
    this.attemptFailedObserver = attemptFailedObserver;
    this.errorHandler = errorHandler;
    this.deadLetterTopic = deadLetterTopic;
    this.kpipeProducer = kpipeProducer;
    this.tracer = tracer;
    this.maxRetries = maxRetries;
    this.retryBackoff = retryBackoff;
    this.consumerActive = consumerActive;
    this.loadReleased = loadReleased;
    if (batchSpecs.isEmpty()) {
      this.batchWrappers = Map.of();
    } else {
      final var wrappers = new LinkedHashMap<String, BatchPipelineWrapper<?>>(batchSpecs.size());
      for (final var spec : batchSpecs) wrappers.put(spec.topic(), createBatchWrapper(spec, scheduler));
      this.batchWrappers = Map.copyOf(wrappers);
    }
  }

  /// The per-topic batch wrappers this processor created from the builder's batch specs. The
  /// owning consumer starts them in `start()`, drains them at teardown, and sums their
  /// `bufferedCount()` into its in-flight total; the processor itself routes records into them
  /// from [#tryEnqueueBatchRecord].
  Map<String, BatchPipelineWrapper<?>> batchWrappers() {
    return batchWrappers;
  }

  private <T> BatchPipelineWrapper<T> createBatchWrapper(
    final KPipeConsumerBuilder.BatchSpec<T> spec,
    final ScheduledExecutorService scheduler
  ) {
    // Batch flushes call the OffsetManager directly rather than going through commandQueue. The
    // queue exists to serialize Kafka-consumer calls (pause/resume/commitSync) on the consumer
    // thread; OffsetManager.markOffsetProcessed is already thread-safe and avoiding the queue
    // means the shutdown drain works even after the consumer thread has exited.
    final var callbacks = new BatchPipelineWrapper.BatchCallbacks() {
      @Override
      public void markProcessed(final ConsumerRecord<byte[], byte[]> record) {
        counters.topic(record.topic()).processed.increment();
        // Feed the circuit breaker / health window so batch outcomes count toward the failure
        // rate, exactly like the per-record path. Both branches must report, or the rolling
        // window would see only failures and trip spuriously.
        health.recordOutcome(true);
        markOffsetProcessed(record);
      }

      @Override
      public boolean consumerClosing() {
        return !consumerActive.getAsBoolean();
      }

      @Override
      public void onBufferReleased() {
        loadReleased.run();
      }

      @Override
      public void onBatchFailure(
        final ConsumerRecord<byte[], byte[]> record,
        final Exception cause,
        final int retries
      ) {
        attemptFailed(record, cause, retries);
        LOGGER.log(Level.WARNING, () -> "Batch failure for record at offset " + record.offset(), cause);
        failTerminally(record, cause, retries);
      }
    };
    return new BatchPipelineWrapper<>(spec.topic(), spec.pipeline(), spec.sink(), spec.policy(), scheduler, callbacks);
  }

  /// Processes a single Kafka consumer record using the topic's configured pipeline. Runs in the
  /// current virtual thread; retries on exception according to `maxRetries` + `retryBackoff`. On
  /// success the per-record outcome feeds the circuit-breaker window; once retries are exhausted,
  /// or at once for a non-retryable failure, the record is routed to the DLQ (when configured)
  /// and the error handler is invoked.
  ///
  /// Counts the record as received on entry and, once its attempts end, as processed or
  /// filtered; failures are counted on the error path and retries in the retry loop.
  ///
  /// @param record the Kafka consumer record to process
  void process(final ConsumerRecord<byte[], byte[]> record) {
    final var topic = counters.topic(record.topic());
    topic.received.increment();

    Tracer.SpanScope span;
    try {
      span = tracer.startConsumerSpan(record);
    } catch (final Exception traceEx) {
      LOGGER.log(Level.WARNING, "Tracer.startConsumerSpan threw", traceEx);
      span = Tracer.SpanScope.noop();
    }

    final long startNanos = System.nanoTime();
    try {
      switch (tryProcessRecord(record, span)) {
        case PROCESSED -> {
          final var durationNanos = System.nanoTime() - startNanos;
          topic.processingNanos.add(durationNanos);
          topic.processed.increment();
          exporter.recordProcessingTime(record.topic(), Duration.ofNanos(durationNanos));
        }
        case FILTERED -> topic.filtered.increment();
        case UNROUTED -> topic.filtered.increment();
        case PENDING -> {
        }
      }
    } finally {
      try {
        span.close();
      } catch (final Exception traceEx) {
        LOGGER.log(Level.WARNING, "Tracer.SpanScope.close threw", traceEx);
      }
      // In-flight count + backpressure-unpark handled by the dispatcher's `onComplete`
      // callback (`wakeIfBackpressureDrained`). See `KPipeConsumer.processRecords`.
    }
  }

  private Outcome tryProcessRecord(final ConsumerRecord<byte[], byte[]> record, final Tracer.SpanScope span) {
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
      return Outcome.UNROUTED;
    }
    return runWithRetries(record, span, _ -> {
      final var delivered = driveSinkedPipeline(pipeline, record.value());
      markOffsetProcessed(record);
      health.recordOutcome(true);
      return delivered ? Outcome.PROCESSED : Outcome.FILTERED;
    });
  }

  /// The shared attempt loop behind both route shapes: run `attempt`, and on a thrown exception
  /// back off and re-run up to `maxRetries` times. Each attempt runs with the record bound as
  /// [RecordContext#current()], so every attempt reads the record as it was consumed.
  /// Interruption (of the backoff sleep, or a failure [#interruptedBy] recognises) restores the
  /// flag and aborts without retrying and without invoking the error path — teardown recovery is
  /// re-fetch on restart. Once attempts are exhausted, or at once for a [NonRetryableException]
  /// anywhere in the failure's cause chain, the failure goes terminal through
  /// [#handleProcessingError] with `retryCount` equal to the retries actually performed.
  private Outcome runWithRetries(
    final ConsumerRecord<byte[], byte[]> record,
    final Tracer.SpanScope span,
    final RecordAttempt attemptBody
  ) {
    final var context = new KafkaRecordContext(record);
    for (int attempt = 0; attempt <= maxRetries; attempt++) {
      if (attempt > 0) {
        counters.topic(record.topic()).retries.increment();
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
          return Outcome.PENDING;
        }
      }

      final int current = attempt;
      try {
        return RecordContext.callWith(context, () -> attemptBody.run(current));
      } catch (final Exception e) {
        if (interruptedBy(e)) {
          Thread.currentThread().interrupt();
          return Outcome.PENDING;
        }
        attemptFailed(record, e, attempt);
        if (attempt == maxRetries || NonRetryableException.isNonRetryable(e)) {
          handleProcessingError(record, e, attempt, span);
          return Outcome.PENDING;
        }
      }
    }
    return Outcome.PENDING;
  }

  /// Batch route: drive the record's deserialize + operator step through the same retry loop as
  /// the per-record path, then buffer the `Passed` value. Only the pre-buffer step is retried —
  /// a record that passes is enqueued exactly once via [#enqueueBuffered], outside the retried
  /// region, so a slow or failing flush can never cause a duplicate buffer entry. Pipeline
  /// `Failed` results rethrow so a transient operator failure gets its configured attempts
  /// before going terminal (DLQ / error handler) with the real retry count.
  private <T> Outcome tryEnqueueBatchRecord(
    final ConsumerRecord<byte[], byte[]> record,
    final BatchPipelineWrapper<T> wrapper,
    final Tracer.SpanScope span
  ) {
    final var pipeline = wrapper.pipeline();
    return runWithRetries(record, span, attempt -> {
      final var deserialized = pipeline.deserializeOrFail(record.value());
      return switch (pipeline.process(deserialized)) {
        case Result.Passed<T> p -> enqueueBuffered(record, wrapper, p.value(), attempt, span);
        case Result.Filtered<T> _ -> {
          // Intentional filter — mark processed immediately; nothing to buffer.
          markOffsetProcessed(record);
          yield Outcome.FILTERED;
        }
        case Result.Failed<T> f -> throw rethrowResultCause(f.cause());
      };
    });
  }

  /// Buffers a `Passed` value into the batch wrapper exactly once. Called from inside the retry
  /// loop but handles its own failures so the loop can never observe an enqueue throw — retrying
  /// an enqueue would re-run deserialize + process and buffer a duplicate. An enqueue failure
  /// (e.g. an inline size-triggered flush path throwing) is terminal for the record: it takes
  /// the standard error path with the retry count accumulated so far. Returns
  /// [Outcome#PENDING] because a buffered record is not yet processed; the flush callback counts
  /// it as processed when the batch reaches the user sink.
  private <T> Outcome enqueueBuffered(
    final ConsumerRecord<byte[], byte[]> record,
    final BatchPipelineWrapper<T> wrapper,
    final T value,
    final int attempt,
    final Tracer.SpanScope span
  ) {
    try {
      wrapper.enqueue(record, value, RecordContext.current(), attempt);
    } catch (final Exception e) {
      if (interruptedBy(e)) {
        Thread.currentThread().interrupt();
        return Outcome.PENDING;
      }
      attemptFailed(record, e, attempt);
      handleProcessingError(record, e, attempt, span);
    }
    return Outcome.PENDING;
  }

  /// Drive a pipeline (with erased element type) from raw bytes to its terminal sink. Throws if
  /// the pipeline reports `Failed` so the calling retry/error path handles it.
  ///
  /// @return true when the record passed and its sink ran, false when it was filtered
  private static <T> boolean driveSinkedPipeline(final Pipeline<T> pipeline, final byte[] data) {
    final var deserialized = pipeline.deserializeOrFail(data);
    return switch (pipeline.process(deserialized)) {
      case Result.Passed<T> p -> {
        final var sink = pipeline.getSink();
        if (sink != null) sink.accept(p.value());
        yield true;
      }
      case Result.Filtered<T> _ -> false;
      case Result.Failed<T> f -> throw rethrowResultCause(f.cause());
    };
  }

  /// Re-throw a captured `Result.Failed` cause as an unchecked exception so the retry/error path
  /// can catch it. Mirrors the legacy Pipeline byte-level entry point behavior — it just
  /// lives here now, where the catching happens, rather than buried in three duplicated unwrap
  /// blocks inside Pipeline.
  private static RuntimeException rethrowResultCause(final Throwable cause) {
    if (cause instanceof RuntimeException re) return re;
    if (cause instanceof Error err) throw err;
    return new RuntimeException(cause);
  }

  /// Marks `record` with the offset manager. A manager that throws is logged and changes nothing
  /// else: the record is not retried or failed again, and its offset stays pending, so the commit
  /// point holds there until the next restart or rebalance.
  private void markOffsetProcessed(final ConsumerRecord<byte[], byte[]> record) {
    if (offsetManager == null) return;
    try {
      offsetManager.markOffsetProcessed(record);
    } catch (final Exception e) {
      // Exception, not RuntimeException: a manager can throw a checked exception it does not
      // declare, and letting it escape would fail a record that was already processed.
      LOGGER.log(Level.ERROR, "OffsetManager.markOffsetProcessed threw at offset " + record.offset(), e);
    }
  }

  /// Reports one failed attempt to the attempt observer, with no record bound, as the error
  /// handler runs. An observer that throws, an `AssertionError` included, is logged and cannot
  /// change what happens to the record; only the JVM's own fatal errors pass through.
  private void attemptFailed(final ConsumerRecord<byte[], byte[]> record, final Exception cause, final int retryCount) {
    final var observer = attemptFailedObserver;
    if (observer == null) return;
    try {
      RecordContext.callWithout(() -> {
        observer.accept(new KPipeConsumer.ProcessingError(record, cause, retryCount));
        return null;
      });
    } catch (final VirtualMachineError fatal) {
      throw fatal;
    } catch (final Throwable ex) {
      LOGGER.log(Level.WARNING, () -> "Attempt observer threw at offset " + record.offset(), ex);
    }
  }

  private void handleProcessingError(
    final ConsumerRecord<byte[], byte[]> record,
    final Exception e,
    final int retryCount,
    final Tracer.SpanScope span
  ) {
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
    failTerminally(record, e, retryCount);
  }

  /// Takes a failed record to its terminal state; the per-record path and a batch flush both end
  /// here. The record counts as failed, is parked, feeds the circuit breaker, and reaches the
  /// error handler, in that order.
  ///
  /// The record adds one failure to the circuit breaker's window when a retry might have fixed
  /// it, or when its dead-letter send failed. A record that cannot succeed but is safely parked
  /// says nothing about downstream health; a dead-letter topic that is down is a downstream
  /// outage like any other.
  private void failTerminally(final ConsumerRecord<byte[], byte[]> record, final Exception cause, final int retries) {
    counters.topic(record.topic()).failed.increment();
    final var parked = park(record, cause);
    if (!parked || !NonRetryableException.isNonRetryable(cause)) health.recordOutcome(false);
    try {
      // A failure can reach here from inside an attempt, where the record is bound; the error
      // handler gets the record in the ProcessingError and never has a current record.
      RecordContext.callWithout(() -> {
        errorHandler.accept(new KPipeConsumer.ProcessingError(record, cause, retries));
        return null;
      });
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

  /// Parks a failed record in the dead-letter topic, or advances past it when none is configured,
  /// which opts into log-and-advance. Its offset advances only once the record is in one of those
  /// durable terminal states. A failed send leaves the offset pending, so the commit point holds
  /// and the record, and its send, are retried after the next restart or partition reassignment.
  /// A dead-letter topic that is down stalls the commit point instead of dropping the record,
  /// while the fetch position keeps moving ahead in memory.
  ///
  /// @return `false` only when the dead-letter send failed
  private boolean park(final ConsumerRecord<byte[], byte[]> record, final Exception cause) {
    if (deadLetterTopic == null || kpipeProducer == null) {
      markOffsetProcessed(record);
      return true;
    }
    if (kpipeProducer.sendToDlq(deadLetterTopic, record, record.topic(), cause)) {
      counters.topic(record.topic()).dlqSent.increment();
      markOffsetProcessed(record);
      return true;
    }
    counters.topic(record.topic()).dlqFailed.increment();
    LOGGER.log(
      Level.ERROR,
      () ->
        "DLQ delivery failed for record at offset " +
        record.offset() +
        "; offset NOT committed, record will be reprocessed on restart/rebalance"
    );
    return false;
  }

  /// Whether a failure is an interruption to abandon the record for, rather than a failure to
  /// retry and then fail: this thread's interrupt flag is set, or the consumer is shutting down
  /// and the failure came from an interruption. While the consumer runs, an interruption inside a
  /// pipeline's exception came from the pipeline's own machinery, not from shutdown; abandoning the
  /// record for it would leave it unmarked and set a fake interrupt on the processing thread.
  private boolean interruptedBy(final Exception failure) {
    if (Thread.currentThread().isInterrupted()) return true;
    return !consumerActive.getAsBoolean() && Interruptions.isInterruptionRelated(failure);
  }

  /// Surfaces a rejection from `ParallelDispatcher`'s executor (typically during shutdown)
  /// back to the consumer's error path. The record never started processing, so it is neither
  /// marked processed nor DLQ'd — at shutdown, re-fetch on restart is the right recovery, not a
  /// poisoned mark.
  void handleParallelRejection(final ConsumerRecord<byte[], byte[]> record, final RejectedExecutionException e) {
    if (!consumerActive.getAsBoolean()) return;
    LOGGER.log(Level.WARNING, "Task submission rejected while the consumer was running", e);
    final var topic = counters.topic(record.topic());
    topic.received.increment();
    topic.failed.increment();
    try {
      errorHandler.accept(new KPipeConsumer.ProcessingError(record, e, 0));
    } catch (final Exception ex) {
      LOGGER.log(
        Level.ERROR,
        () -> "Error handler threw while handling rejected task at offset " + record.offset(),
        ex
      );
    }
  }
}
