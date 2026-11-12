package io.github.eschizoid.kpipe;

import io.github.eschizoid.kpipe.consumer.CircuitBreakerController;
import io.github.eschizoid.kpipe.consumer.KPipeConsumer;
import io.github.eschizoid.kpipe.consumer.MetricsExporter;
import io.github.eschizoid.kpipe.consumer.OffsetStore;
import io.github.eschizoid.kpipe.consumer.ProcessingMode;
import io.github.eschizoid.kpipe.registry.Result;
import io.github.eschizoid.kpipe.registry.SchemaResolver;
import io.github.eschizoid.kpipe.sink.BatchPolicy;
import io.github.eschizoid.kpipe.sink.BatchSink;
import io.github.eschizoid.kpipe.sink.MessageSink;
import io.github.eschizoid.kpipe.tracing.Tracer;
import java.time.Duration;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;

/// Fluent stream-composition type for the [KPipe] facade.
///
/// A `Stream<T>` accumulates configuration (operators, retry, backpressure, processing mode)
/// against a logical Kafka topic + format and is terminated by a `to*` method that returns a
/// [Sink] ready to `start()`.
///
/// **Immutability contract:** every builder method returns a NEW `Stream<T>` carrying the
/// updated configuration. The original instance is never mutated. This makes branching from a
/// common root safe:
///
/// ```java
/// final var root = KPipe.json("events", props);
/// final var enriched = root.pipe(addTimestamp);   // root remains untouched
/// final var sanitized = root.pipe(JsonFields.remove("password")); // independent of `enriched`
/// ```
///
/// Standard fluent usage chains the calls so the intermediate instances are not visible:
///
/// ```java
/// KPipe.json("events", kafkaProps)
///     .pipe(msg -> { msg.put("ts", System.currentTimeMillis()); return msg; })
///     .filter(msg -> "active".equals(msg.get("status")))
///     .toConsole()
///     .start();
/// ```
///
/// @param <T> the deserialized message type flowing through the pipeline
public interface Stream<T> {
  /// Returns a new stream with `op` appended to the pipeline.
  ///
  /// @param op the operator to append (must not be null). Returning `null` from the operator
  ///     is treated as intentional filtering — downstream operators are skipped for that
  ///     message (see [io.github.eschizoid.kpipe.registry.MessagePipeline] error semantics).
  /// @return a new stream with `op` appended
  /// @throws NullPointerException if `op` is null
  Stream<T> pipe(final UnaryOperator<T> op);

  /// Returns a new stream with a filter operator appended. Messages for which `keep.test(msg)`
  /// returns false are dropped (the operator returns null, short-circuiting downstream).
  /// Equivalent to `pipe(Operators.filter(keep))`.
  ///
  /// @param keep predicate; messages for which it returns true are passed downstream
  /// @return a new stream with the filter appended
  /// @throws NullPointerException if `keep` is null
  Stream<T> filter(final Predicate<T> keep);

  /// Returns a new stream that runs `sideEffect` on each message and passes it through
  /// unchanged. Useful for logging, metrics, or other observation. Equivalent to
  /// `pipe(Operators.peek(sideEffect))`.
  ///
  /// @param sideEffect the side-effect to run on each message
  /// @return a new stream with the side-effect appended
  /// @throws NullPointerException if `sideEffect` is null
  Stream<T> peek(final Consumer<T> sideEffect);

  /// Returns a new stream with a conditional-branch operator appended. Each message is sent
  /// through `ifTrue` when `cond.test(msg)` is true, otherwise through `ifFalse`.
  ///
  /// @param cond the condition predicate
  /// @param ifTrue operator applied when `cond.test(msg)` is true
  /// @param ifFalse operator applied when `cond.test(msg)` is false
  /// @return a new stream with the branch appended
  /// @throws NullPointerException if any argument is null
  Stream<T> when(final Predicate<T> cond, final UnaryOperator<T> ifTrue, final UnaryOperator<T> ifFalse);

  /// Replaces the record's key for the rest of the attempt. Later operators and sinks read the new
  /// key from `RecordContext.current().key()`, and [#toKafka] writes it. Dispatch in `KEY_ORDERED`
  /// mode, the error handler and the dead-letter topic use the consumed key, and a retry starts
  /// from it again. A batch sink reads the new key through `RecordContext.currentBatch()`.
  ///
  /// @param keyFn computes the new key from the value; null means no key
  /// @return a new [Stream] with the operator appended
  /// @throws NullPointerException if `keyFn` is null
  Stream<T> rekey(final Function<? super T, byte[]> keyFn);

  /// [#rekey] with the key given as a string and encoded as UTF-8.
  ///
  /// @param keyFn computes the new key from the value; null means no key
  /// @return a new [Stream] with the operator appended
  /// @throws NullPointerException if `keyFn` is null
  Stream<T> rekeyString(final Function<? super T, String> keyFn);

  /// Adds a header for the rest of the attempt. Later operators and sinks read it through
  /// `RecordContext.current()`, and [#toKafka] writes it after the consumed record's headers. The
  /// consumed record's headers never change, so the dead-letter topic and a retry do not see it. A
  /// batch sink reads it through `RecordContext.currentBatch()`.
  ///
  /// @param name    the header name
  /// @param valueFn computes the header value from the value
  /// @return a new [Stream] with the operator appended
  /// @throws NullPointerException if `name` or `valueFn` is null
  Stream<T> withHeader(final String name, final Function<? super T, byte[]> valueFn);

  /// Returns a new stream with retry behavior configured. After a processing exception, the
  /// pipeline retries up to `maxRetries` times, waiting `backoff` between attempts. When
  /// `maxRetries` is 0 (the default) the message goes straight to the error path / DLQ. A
  /// `NonRetryableException`, or a failure caused by one, is not retried and goes straight to
  /// the error handler / DLQ.
  ///
  /// @param maxRetries maximum number of retry attempts (must be ≥ 0)
  /// @param backoff duration to wait between attempts (must be non-null)
  /// @return a new stream with the retry policy configured
  /// @throws IllegalArgumentException if `maxRetries` is negative
  /// @throws NullPointerException if `backoff` is null
  Stream<T> withRetry(final int maxRetries, final Duration backoff);

  /// Returns a new stream with backpressure enabled using default watermarks
  /// (`BackpressureController.DEFAULT_HIGH_WATERMARK` for pause,
  /// `BackpressureController.DEFAULT_LOW_WATERMARK` for resume). The strategy is in-flight in
  /// parallel mode and consumer-lag in sequential mode.
  ///
  /// Backpressure is enabled by default; calling this method is only required when you want
  /// to override the watermarks via [#withBackpressure(long, long)].
  ///
  /// @return a new stream with backpressure configured
  Stream<T> withBackpressure();

  /// Returns a new stream with backpressure enabled using explicit watermarks. Hysteresis: the
  /// consumer pauses Kafka polling when the metric reaches `high` and resumes when it drops to
  /// or below `low`.
  ///
  /// @param high pause threshold (must be > 0)
  /// @param low resume threshold (must be ≥ 0 and strictly less than `high`)
  /// @return a new stream with the backpressure thresholds configured
  /// @throws IllegalArgumentException if the watermarks are invalid
  Stream<T> withBackpressure(final long high, final long low);

  /// Returns a new stream with the processing mode set. See [ProcessingMode] for semantics:
  ///
  /// - `SEQUENTIAL`: one message at a time per partition, in offset order. Lag-based
  ///   backpressure.
  /// - `PARALLEL` (default): virtual thread per record. No ordering guarantees. In-flight
  ///   backpressure.
  /// - `KEY_ORDERED`: per-key serial processing on a virtual thread per key, with a key cap
  ///   (default 10,000 keys; override with [#withKeyOrderedMaxKeys]). Records with the same
  ///   key process in order; different keys process in parallel. In-flight backpressure.
  ///
  /// @param mode the processing mode (must be non-null)
  /// @return a new stream with the processing mode configured
  /// @throws NullPointerException if `mode` is null
  Stream<T> withProcessingMode(final ProcessingMode mode);

  /// Sets the cap on distinct keys held in-memory simultaneously when this stream uses
  /// [ProcessingMode#KEY_ORDERED]. Default 10,000. Has no effect for `SEQUENTIAL` or
  /// `PARALLEL` modes.
  ///
  /// @param maxKeys distinct-key cap (must be positive)
  /// @return a new stream with the key cap configured
  /// @throws IllegalArgumentException if `maxKeys` is non-positive
  Stream<T> withKeyOrderedMaxKeys(final int maxKeys);

  /// Returns a new stream that exports its metrics to a monitoring backend, such as OpenTelemetry
  /// through `OtelMetrics.of(openTelemetry)` from `kpipe-metrics-otel`. Without an exporter the
  /// metrics are still counted and readable through `Handle.metrics()`.
  ///
  /// @param exporter the exporter (must not be null)
  /// @return a new stream with the exporter attached
  /// @throws NullPointerException if `exporter` is null
  Stream<T> withMetrics(final MetricsExporter exporter);

  /// Returns a new stream with a [Tracer] attached for W3C trace-context propagation. The tracer
  /// starts a consumer span around each record (extracting upstream context from headers) and
  /// injects the active context into DLQ and producer-sink outbound records.
  ///
  /// Use `new OtelTracer(openTelemetry, "my-pipeline")` from `kpipe-tracing-otel` for OpenTelemetry
  /// wiring. Pass [Tracer#noop] to disable tracing explicitly (also the default when this method
  /// is not called).
  ///
  /// @param tracer the tracer implementation (must not be null; use `Tracer.noop()` to disable)
  /// @return a new stream with tracing attached
  /// @throws NullPointerException if `tracer` is null
  Stream<T> withTracer(final Tracer tracer);

  /// Returns a new stream with a circuit breaker attached. Watches per-record outcomes; once the
  /// failure rate over the sliding window crosses `failureThreshold`, pauses Kafka polling until
  /// `openDuration` elapses, then resumes and treats the next record as a probe (success
  /// returns to closed, failure trips again and restarts the timer).
  ///
  /// **Why CB and not just retries.** Retries handle transient per-record failures. CB handles
  /// sustained outages — DB down, downstream 503-storm — by stopping work entirely until the
  /// dependency recovers. Without it, every record during an outage exhausts `maxRetries` and
  /// floods the DLQ.
  ///
  /// @param failureThreshold the failure rate (0.0..1.0) at which to trip
  /// @param windowSize the rolling sample size — must be positive
  /// @param openDuration how long to stay in OPEN before probing
  /// @return a new stream with the breaker configured
  /// @throws IllegalArgumentException for invalid arguments
  Stream<T> withCircuitBreaker(final double failureThreshold, final int windowSize, final Duration openDuration);

  /// Returns a new stream with an explicit [CircuitBreakerController]. Use this form when you
  /// need to share a pre-built controller across consumers or when constructing it from
  /// configuration. For inline use prefer the three-argument overload.
  ///
  /// @param controller the controller (must not be null)
  /// @return a new stream with the breaker configured
  /// @throws NullPointerException if `controller` is null
  Stream<T> withCircuitBreaker(final CircuitBreakerController controller);

  /// Returns a new stream that invokes `observer` whenever a record is intentionally filtered
  /// (an operator returned `null`). The observer is for visibility — it does **not** affect
  /// pipeline flow, suppress the filter, or reroute the record. Use [#withErrorHandler] or
  /// [#withDeadLetterTopic] for real failure routing; this is purely a hook for logging and
  /// metrics.
  ///
  /// Runs on the same thread that processes the record: the consumer thread in `SEQUENTIAL`
  /// mode, or a virtual worker thread in `PARALLEL` / `KEY_ORDERED` mode. If the observer
  /// throws, the exception is logged at WARNING and swallowed so observer bugs cannot crash the
  /// pipeline. Calling this method more than once replaces the previous observer — for fan-out,
  /// compose externally.
  ///
  /// @param observer a side-effect invoked when a record is filtered (must not be null)
  /// @return a new stream with the observer attached
  /// @throws NullPointerException if `observer` is null
  Stream<T> onFiltered(final Runnable observer);

  /// Returns a new stream that invokes `observer` with the captured cause whenever the pipeline
  /// returns `Failed`. The observer is for visibility — it does **not** suppress the failure or
  /// stop the failure from reaching retry / DLQ / `withErrorHandler` logic.
  ///
  /// Fires on **every** failing pipeline invocation — including each retry attempt, so a record
  /// that exhausts `withRetry(2, ...)` invokes this observer three times. For exactly one callback
  /// per record after retries are exhausted, use `withErrorHandler(...)` instead.
  ///
  /// Runs on the same thread that processes the record: the consumer thread in `SEQUENTIAL`
  /// mode, or a virtual worker thread in `PARALLEL` / `KEY_ORDERED` mode. If the observer
  /// throws, the exception is logged at WARNING and swallowed. Calling this method more than
  /// once replaces the previous observer.
  ///
  /// **Use this when records stop reaching the sink.** If `metrics().records().failed()` or
  /// `filtered()` keeps rising while the sink sees nothing, attach
  /// `onFailed(cause -> log.warn("pipeline failed", cause))` to surface what's actually being
  /// lost.
  ///
  /// @param observer a side-effect invoked with the failure cause (must not be null)
  /// @return a new stream with the observer attached
  /// @throws NullPointerException if `observer` is null
  Stream<T> onFailed(final Consumer<Throwable> observer);

  /// Returns a new stream that invokes `observer` with every [Result] the pipeline produces —
  /// `Passed`, `Filtered`, or `Failed`. The lowest-level observer hook; use this for full
  /// pattern-matching diagnostic taps. Does **not** affect pipeline flow.
  ///
  /// Runs on the same thread that processes the record, before any downstream sink invocation:
  /// the consumer thread in `SEQUENTIAL` mode, or a virtual worker thread in `PARALLEL` /
  /// `KEY_ORDERED` mode. If the observer throws, the exception is logged at WARNING and
  /// swallowed. Calling this method more than once replaces the previous observer.
  ///
  /// @param observer a side-effect invoked with every pipeline outcome (must not be null)
  /// @return a new stream with the observer attached
  /// @throws NullPointerException if `observer` is null
  Stream<T> peekResult(final Consumer<Result<T>> observer);

  /// Returns a new stream with a custom error handler. The handler is invoked after retries are
  /// exhausted (or immediately when `maxRetries == 0`) with the failing record, the exception,
  /// and the retry count. The default handler logs at WARNING.
  ///
  /// @param handler the error handler (must not be null)
  /// @return a new stream with the error handler attached
  /// @throws NullPointerException if `handler` is null
  Stream<T> withErrorHandler(final Consumer<KPipeConsumer.ProcessingError> handler);

  /// Returns a new stream with a dead-letter topic configured. Records that fail processing after
  /// retries are exhausted are forwarded to `dlqTopic` (using a `KPipeProducer` derived from the
  /// consumer properties) before the error handler is invoked.
  ///
  /// @param dlqTopic the dead-letter topic name (must be non-blank)
  /// @return a new stream with the DLQ configured
  /// @throws IllegalArgumentException if `dlqTopic` is null or blank
  Stream<T> withDeadLetterTopic(final String dlqTopic);

  /// Returns a new stream with a custom Kafka poll timeout. The default is 100ms; longer
  /// timeouts reduce CPU usage on idle topics, shorter timeouts make backpressure / shutdown
  /// commands more responsive.
  ///
  /// @param timeout the poll timeout (must be non-null and non-negative)
  /// @return a new stream with the poll timeout configured
  /// @throws NullPointerException if `timeout` is null
  Stream<T> withPollTimeout(final Duration timeout);

  /// Returns a new stream that commits the offset frontier at this interval. The default is 30
  /// seconds. Offsets are also committed on rebalance and at shutdown; a shorter interval narrows
  /// how many records are reprocessed after a crash, at the cost of more commit requests.
  ///
  /// @param interval the time between periodic commits (non-null, at least 1 millisecond)
  /// @return a new stream with the commit interval configured
  /// @throws NullPointerException if `interval` is null
  Stream<T> withCommitInterval(final Duration interval);

  /// Returns a new stream that keeps its committed offsets in `store` instead of Kafka. The
  /// consumer still commits the offset frontier on the same schedule; on assignment it seeks each
  /// partition to the offset the store holds. See [OffsetStore] for what a store must guarantee.
  ///
  /// @param store where the consumer group's offsets live (non-null)
  /// @return a new stream with the offset store configured
  /// @throws NullPointerException if `store` is null
  Stream<T> withOffsetStore(final OffsetStore store);

  /// Returns a new stream that logs one line of the consumer's metrics every `interval`, plus a
  /// final line when it closes, after its last records are counted. The line goes to the
  /// `io.github.eschizoid.kpipe.metrics.log` logger at INFO, as `key=value` pairs whose keys follow
  /// the OpenTelemetry instrument names, with times in milliseconds:
  ///
  /// ```text
  /// kpipe metrics [orders group=billing] received=120 processed=117 filtered=1 failed=2 ...
  /// ```
  ///
  /// It needs no monitoring backend and is independent of [#withMetrics].
  ///
  /// @param interval how often to log (must be positive)
  /// @return a new stream with the metrics log configured
  /// @throws NullPointerException if `interval` is null
  /// @throws IllegalArgumentException if `interval` is zero or negative
  Stream<T> withMetricsLog(final Duration interval);

  /// Returns a new stream that registers a JVM shutdown hook to close the consumer.
  ///
  /// A caller using the [Handle] in try-with-resources is already covered on the normal exit
  /// path; the hook is what covers a SIGTERM, which is how most container runtimes stop a process.
  ///
  /// @param useShutdownHook whether to register the hook
  /// @return a new stream with the shutdown hook configured
  Stream<T> withShutdownHook(final boolean useShutdownHook);

  /// Returns a new stream that waits at most `timeout` for the consumer thread to terminate
  /// during shutdown. Unset, the wait defaults per processing mode: 30 seconds for `PARALLEL`, 25
  /// for `KEY_ORDERED`, 15 for `SEQUENTIAL`, and 5 more with a dead-letter queue or Kafka sink.
  ///
  /// @param timeout the thread-join budget (must be non-null and non-negative)
  /// @return a new stream with the termination timeout configured
  /// @throws NullPointerException if `timeout` is null
  Stream<T> withThreadTerminationTimeout(final Duration timeout);

  /// Returns a new stream that waits at most `timeout` for in-flight records to drain during
  /// shutdown. Distinct from [Handle#shutdownGracefully], which bounds one specific drain call;
  /// this sets the consumer's own budget.
  ///
  /// @param timeout the drain budget (must be non-null and non-negative)
  /// @return a new stream with the drain timeout configured
  /// @throws NullPointerException if `timeout` is null
  Stream<T> withWaitForMessagesTimeout(final Duration timeout);

  /// Returns a new stream that skips the first `n` bytes of every payload before deserialization.
  /// Useful for stripping wire-format prefixes — e.g. Confluent Schema Registry's 5-byte envelope
  /// (1 magic + 4 schema id) for Avro, or 6-byte envelope (5 + 1 message-index varint) for the
  /// single-top-level-message Protobuf case.
  ///
  /// Prefer [#withSchemaRegistry(SchemaResolver)] for Avro on Confluent SR — it consumes the
  /// envelope and resolves the correct writer schema per record, which is also what makes
  /// schema-evolution-correct decoding work. `skipBytes(5)` only strips the prefix; it does not
  /// fix the static-reader-schema correctness hole described in the project README.
  ///
  /// @param n the number of leading bytes to skip (must be ≥ 0)
  /// @return a new stream with the skip configured
  /// @throws IllegalArgumentException if `n` is negative
  Stream<T> skipBytes(final int n);

  /// Switches the stream's format into Schema-Registry mode. Supported for **Avro** and
  /// **Protobuf** — each reads its own Confluent wire envelope, resolves the writer schema per
  /// record via `resolver` (cache it with `CachedSchemaResolver` from
  /// `kpipe-schema-registry-confluent`), and decodes the remaining bytes against it. Do NOT
  /// combine with `skipBytes` — the format already strips the envelope (5 bytes for Avro, 6+ for
  /// Protobuf including the message-index).
  ///
  /// **Runtime modules.** Avro needs `kpipe-schema-registry-confluent` for the resolver; Protobuf
  /// additionally needs **`kpipe-format-protobuf-confluent`** on the runtime path — it supplies
  /// the `.proto`-text compiler discovered via `ServiceLoader` (protobuf-java has no `.proto`
  /// parser, so the compiler ships in a separate shaded module). Calling this on a Protobuf
  /// stream without that module on the path throws at first use with a message telling you to
  /// add it. That compiler is a ~19 MB
  /// shaded jar; only SR-Protobuf users pull it — static `new ProtobufFormat(descriptor)` needs
  /// nothing beyond `protobuf-java`.
  ///
  /// **Formats are opt-in.** `kpipe-api` declares the format modules `requires static`, so a
  /// registry (or any format) call needs the matching `kpipe-format-*` module on the runtime
  /// path or you get a `NoClassDefFoundError`.
  ///
  /// @param resolver the schema resolver (typically a `CachedSchemaResolver` wrapping a
  ///                 `ConfluentSchemaResolver`)
  /// @return a new stream with the registry-backed format wired in
  /// @throws NullPointerException if `resolver` is null
  /// @throws UnsupportedOperationException if the underlying format does not support
  ///         registry-backed mode
  Stream<T> withSchemaRegistry(final SchemaResolver resolver);

  /// Terminates the stream with a format-appropriate console sink and returns a [Sink] ready
  /// to start. Each format supplies its own console sink; a [KPipe#custom] stream logs
  /// `String.valueOf(value)`.
  ///
  /// A **registry-mode** Avro/Protobuf stream (via [#withSchemaRegistry]) has no fixed
  /// schema/descriptor, so the console sink is unsupported there — `toConsole()` throws; use
  /// `.toCustom(...)` with your own sink instead.
  ///
  /// @return a [Sink] ready to start
  /// @throws IllegalStateException if the stream is in registry mode
  Sink<T> toConsole();

  /// Terminates the stream with a user-provided sink and returns a [Sink] ready to start.
  ///
  /// @param sink the terminal sink (must not be null)
  /// @return a [Sink] ready to start
  /// @throws NullPointerException if `sink` is null
  Sink<T> toCustom(final MessageSink<T> sink);

  /// Terminates the stream by producing each value to `topic`, serialized with this stream's
  /// format. Each output record carries the key and headers of the record it came from, after any
  /// [#rekey] or [#withHeader]. With the default partitioner, records with the same non-null key
  /// go to one output partition. Within one output partition, records consumed with the same key
  /// arrive in the order they were consumed in `KEY_ORDERED` and `SEQUENTIAL` mode, and in
  /// `SEQUENTIAL` mode every record does.
  ///
  /// The producer is built from this stream's Kafka properties when the stream starts, the same
  /// way the dead-letter producer is, and closes with the consumer. Each send waits for the
  /// broker's acknowledgement. A failed send fails the record, which is retried when retries are
  /// configured, then handed to the error handler and, when one is configured, the dead-letter
  /// topic. Bytes removed with [#skipBytes] on input are not written back.
  ///
  /// @param topic the topic to produce to
  /// @return a [Sink] ready to start
  /// @throws NullPointerException if `topic` is null
  /// @throws IllegalArgumentException if `topic` is blank
  /// @throws IllegalStateException if the stream reads through a Schema Registry, whose format
  /// cannot serialize
  Sink<T> toKafka(final String topic);

  /// Terminates the stream with a buffering [BatchSink], flushing in chunks of `policy.maxSize()`
  /// or whenever `policy.maxAge()` elapses since the oldest buffered record. The sink returns a
  /// [io.github.eschizoid.kpipe.sink.BatchResult] naming per-record outcomes: succeeded records
  /// have their
  /// offsets marked processed; failed records are routed to the configured DLQ. For void-style
  /// consumers that report whole-batch success or failure (e.g. a transactional commit), wrap
  /// with [BatchSink#ofVoid] — exceptions translate to `BatchResult.allFailed`.
  ///
  /// **Coverage contract.** The returned [io.github.eschizoid.kpipe.sink.BatchResult] must account
  /// for every
  /// position in the input batch. Indexes that are not named in either the success list or the
  /// failure map are treated as failures (with a synthetic [IllegalStateException] as the
  /// cause) — silently marking them processed would mask sink bugs and risk data loss. If the
  /// sink itself throws, every record is sent to the DLQ with the thrown exception (whole-batch
  /// fallback).
  ///
  /// **Reading the records.** The sink receives values. It reads the records they came from with
  /// `RecordContext.currentBatch()`, index-aligned with the values and including any [#rekey] or
  /// [#withHeader]; `RecordContext.current()` throws there, because the batch covers many records.
  ///
  /// **All processing modes are supported.** Default is parallel; call [#withProcessingMode]
  /// to switch to per-partition or per-key ordering. Under the in-flight-backpressure modes
  /// (`PARALLEL` and `KEY_ORDERED`), buffered records count toward the in-flight metric so a
  /// slow sink cannot let the buffer grow unbounded; `SEQUENTIAL` uses lag-based backpressure
  /// instead. For multi-topic batch consumers, use the [KPipe#multi] builder.
  ///
  /// @param sink the batch sink invoked on each flush (must not be null)
  /// @param policy the size/age flush thresholds (must not be null)
  /// @return a [Sink] ready to start
  /// @throws NullPointerException if any argument is null
  Sink<T> toBatch(final BatchSink<T> sink, final BatchPolicy policy);

  /// Terminates the stream by fanning out to multiple sinks. Each delivered message is dispatched
  /// to every sink; an exception in one sink is **logged at WARNING but suppressed** so the
  /// remaining sinks still receive the message (see
  /// [io.github.eschizoid.kpipe.sink.CompositeMessageSink]).
  ///
  /// **Best-effort delivery, NOT at-least-once-per-sink.** Because exceptions are suppressed,
  /// a sink that consistently fails will silently drop messages while the consumer continues to
  /// mark them as processed and commit offsets. The consumer's error path / DLQ is not invoked
  /// for per-sink failures — only the per-sink WARNING log.
  ///
  /// If you need delivery to every sink, do not use `toMulti`. Pass `toCustom` one sink that calls
  /// each in turn: a failure in any of them then fails the record, which goes to retry, the error
  /// handler and the DLQ, and on a retry the sinks that already succeeded receive it again.
  ///
  /// @param sinks the sinks to broadcast to (must contain at least one element)
  /// @return a [Sink] ready to start
  /// @throws IllegalArgumentException if `sinks` is empty
  /// @throws NullPointerException if `sinks` is null
  @SuppressWarnings("unchecked")
  Sink<T> toMulti(final MessageSink<T>... sinks);
}
