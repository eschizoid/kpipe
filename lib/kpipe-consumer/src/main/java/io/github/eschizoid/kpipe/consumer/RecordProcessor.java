package io.github.eschizoid.kpipe.consumer;

import io.github.eschizoid.kpipe.metrics.ConsumerMetricKeys;
import io.github.eschizoid.kpipe.metrics.ConsumerMetrics;
import io.github.eschizoid.kpipe.producer.KPipeProducer;
import io.github.eschizoid.kpipe.producer.tracing.Tracer;
import io.github.eschizoid.kpipe.registry.MessagePipeline;
import io.github.eschizoid.kpipe.registry.Result;
import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.nio.channels.ClosedByInterruptException;
import java.time.Duration;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicLong;
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
/// the consumer (the metrics map, the offset manager, the health controller, the DLQ producer)
/// — so [#process] is safe to invoke from any dispatcher worker thread.
///
/// Retry semantics are uniform across route shapes: both the regular per-record path and the
/// batch route drive their deserialize + operator step through the same attempt loop
/// ([#runWithRetries]). On the batch route only the pre-buffer step is retried; a successful
/// attempt buffers the value exactly once, and the batch sink flush itself is never retried
/// (flush failures take the `BatchResult` / DLQ path in the flush callbacks).
final class RecordProcessor {

  private static final Logger LOGGER = System.getLogger(RecordProcessor.class.getName());

  // Aliases of the shared public key set (ConsumerMetricKeys) — the single source of truth also
  // read by ConsumerMetricsReporter and the kpipe-test quiescence check. Never re-declare the
  // literals here; aliasing keeps every call site short while making drift impossible.
  private static final String METRIC_MESSAGES_RECEIVED = ConsumerMetricKeys.MESSAGES_RECEIVED;
  private static final String METRIC_MESSAGES_PROCESSED = ConsumerMetricKeys.MESSAGES_PROCESSED;
  private static final String METRIC_PROCESSING_ERRORS = ConsumerMetricKeys.PROCESSING_ERRORS;
  private static final String METRIC_PROCESSING_DURATION_TOTAL_MS = ConsumerMetricKeys.PROCESSING_DURATION_TOTAL_MS;
  private static final String METRIC_RETRIES = ConsumerMetricKeys.RETRIES;
  private static final String METRIC_DLQ_SENT = ConsumerMetricKeys.DLQ_SENT;
  private static final String METRIC_DLQ_FAILED = ConsumerMetricKeys.DLQ_FAILED;

  private final Map<String, MessagePipeline<?>> pipelines;
  private final Map<String, BatchPipelineWrapper<?>> batchWrappers;
  private final OffsetManager offsetManager;
  private final ConsumerHealthController health;
  private final ConsumerMetrics otelMetrics;
  private final Map<String, AtomicLong> metrics;
  private final KPipeConsumer.ErrorHandler errorHandler;
  private final String deadLetterTopic;
  private final KPipeProducer<byte[], byte[]> kpipeProducer;
  private final Tracer tracer;
  private final int maxRetries;
  private final Duration retryBackoff;
  /// Snapshot of the owning consumer's running state, used only by [#handleParallelRejection]
  /// to stay silent on rejections that are part of an orderly shutdown.
  private final BooleanSupplier consumerActive;

  /// One deserialize + process attempt for a record. Returns the record's terminal-success flag
  /// (the same meaning as [#tryProcessRecord]'s return value); throws to request another
  /// attempt from the surrounding retry loop.
  @FunctionalInterface
  private interface RecordAttempt {
    boolean run(int attempt) throws Exception;
  }

  RecordProcessor(
    final Map<String, MessagePipeline<?>> pipelines,
    final Collection<KPipeConsumerBuilder.BatchSpec<?>> batchSpecs,
    final ScheduledExecutorService scheduler,
    final OffsetManager offsetManager,
    final ConsumerHealthController health,
    final ConsumerMetrics otelMetrics,
    final Map<String, AtomicLong> metrics,
    final KPipeConsumer.ErrorHandler errorHandler,
    final String deadLetterTopic,
    final KPipeProducer<byte[], byte[]> kpipeProducer,
    final Tracer tracer,
    final int maxRetries,
    final Duration retryBackoff,
    final BooleanSupplier consumerActive
  ) {
    this.pipelines = pipelines;
    this.offsetManager = offsetManager;
    this.health = health;
    this.otelMetrics = otelMetrics;
    this.metrics = metrics;
    this.errorHandler = errorHandler;
    this.deadLetterTopic = deadLetterTopic;
    this.kpipeProducer = kpipeProducer;
    this.tracer = tracer;
    this.maxRetries = maxRetries;
    this.retryBackoff = retryBackoff;
    this.consumerActive = consumerActive;
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
        // LOCKSTEP: mirror of the per-record path's DLQ-or-mark block in handleProcessingError
        // —
        // mark the offset only after a successful DLQ send; a failed send leaves it pending so
        // the record is reprocessed, never dropped. Deliberately duplicated (the paths differ
        // on
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
          errorHandler.accept(new KPipeConsumer.ProcessingError(record, cause, 0));
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
  void process(final ConsumerRecord<byte[], byte[]> record) {
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
      // callback (`afterRecordComplete`). See `KPipeConsumer.processRecords`.
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
    return runWithRetries(record, span, _ -> {
      driveSinkedPipeline(pipeline, record.value());
      markOffsetProcessed(record);
      health.recordOutcome(true);
      return true;
    });
  }

  /// The shared attempt loop behind both route shapes: run `attempt`, and on a thrown exception
  /// back off and re-run up to `maxRetries` times. Interruption (of the backoff sleep, or an
  /// interruption-related failure from the attempt itself) restores the flag and aborts without
  /// retrying and without invoking the error path — teardown recovery is re-fetch on restart.
  /// Once attempts are exhausted the failure goes terminal through [#handleProcessingError]
  /// with `retryCount` equal to the retries actually performed.
  private boolean runWithRetries(
    final ConsumerRecord<byte[], byte[]> record,
    final Tracer.SpanScope span,
    final RecordAttempt attemptBody
  ) {
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
        return attemptBody.run(attempt);
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

  /// Batch route: drive the record's deserialize + operator step through the same retry loop as
  /// the per-record path, then buffer the `Passed` value. Only the pre-buffer step is retried —
  /// a record that passes is enqueued exactly once via [#enqueueBuffered], outside the retried
  /// region, so a slow or failing flush can never cause a duplicate buffer entry. Pipeline
  /// `Failed` results rethrow so a transient operator failure gets its configured attempts
  /// before going terminal (DLQ / error handler) with the real retry count.
  private <T> boolean tryEnqueueBatchRecord(
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
          yield true;
        }
        case Result.Failed<T> f -> throw rethrowResultCause(f.cause());
      };
    });
  }

  /// Buffers a `Passed` value into the batch wrapper exactly once. Called from inside the retry
  /// loop but handles its own failures so the loop can never observe an enqueue throw — retrying
  /// an enqueue would re-run deserialize + process and buffer a duplicate. An enqueue failure
  /// (e.g. an inline size-triggered flush path throwing) is terminal for the record: it takes
  /// the standard error path with the retry count accumulated so far. Returns `false` because a
  /// buffered record is not yet processed — `messagesProcessed` is incremented by the flush
  /// callback when the batch is committed to the user sink.
  private <T> boolean enqueueBuffered(
    final ConsumerRecord<byte[], byte[]> record,
    final BatchPipelineWrapper<T> wrapper,
    final T value,
    final int attempt,
    final Tracer.SpanScope span
  ) {
    try {
      wrapper.enqueue(record, value);
    } catch (final Exception e) {
      if (isInterruptionRelated(e)) {
        Thread.currentThread().interrupt();
        return false;
      }
      handleProcessingError(record, e, attempt, span);
    }
    return false;
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
      errorHandler.accept(new KPipeConsumer.ProcessingError(record, e, retryCount));
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

  /// Surfaces a rejection from `ParallelDispatcher`'s executor (typically during shutdown)
  /// back to the consumer's error path. The record never started processing, so it is neither
  /// marked processed nor DLQ'd — at shutdown, re-fetch on restart is the right recovery, not a
  /// poisoned mark.
  void handleParallelRejection(final ConsumerRecord<byte[], byte[]> record, final RejectedExecutionException e) {
    if (!consumerActive.getAsBoolean()) return;
    LOGGER.log(Level.WARNING, "Task submission rejected during shutdown", e);
    metrics.get(METRIC_PROCESSING_ERRORS).incrementAndGet();
    otelMetrics.recordProcessingError(record.topic());
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

  private static boolean isInterruptionRelated(final Throwable error) {
    for (Throwable current = error; current != null; current = current.getCause()) {
      if (current instanceof InterruptedException || current instanceof ClosedByInterruptException) return true;
    }
    return false;
  }
}
