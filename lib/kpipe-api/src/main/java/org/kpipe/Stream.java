package org.kpipe;

import java.time.Duration;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;
import org.kpipe.sink.MessageSink;

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
/// final var sanitized = root.pipe(removeFields("password")); // independent of `enriched`
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
  ///     message (see [org.kpipe.registry.MessagePipeline] error semantics).
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

  /// Returns a new stream with retry behavior configured. After a processing exception, the
  /// pipeline retries up to `maxRetries` times, waiting `backoff` between attempts. When
  /// `maxRetries` is 0 (the default) the message goes straight to the error path / DLQ.
  ///
  /// @param maxRetries maximum number of retry attempts (must be ≥ 0)
  /// @param backoff duration to wait between attempts (must be non-null)
  /// @return a new stream with the retry policy configured
  /// @throws IllegalArgumentException if `maxRetries` is negative
  /// @throws NullPointerException if `backoff` is null
  Stream<T> withRetry(final int maxRetries, final Duration backoff);

  /// Returns a new stream with backpressure enabled using default watermarks (pause at 10,000
  /// in-flight messages, resume at 7,000). The strategy is in-flight in parallel mode and
  /// consumer-lag in sequential mode.
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

  /// Returns a new stream with the processing mode set. Sequential mode processes one message
  /// per partition at a time and uses lag-based backpressure; parallel mode (the default) uses
  /// virtual threads per record and in-flight backpressure.
  ///
  /// @param sequential true for sequential per-partition processing; false for parallel
  /// @return a new stream with the processing mode configured
  Stream<T> withSequentialProcessing(final boolean sequential);

  /// Terminates the stream with a format-appropriate console sink and returns a [Sink] ready
  /// to start. For Avro streams this requires that a default schema has been registered; see
  /// [KPipe#avro] for details.
  ///
  /// @return a [Sink] ready to start
  /// @throws IllegalStateException if the stream's format requires schema/config that has not
  ///     been registered (e.g. Avro without a default schema)
  Sink<T> toConsole();

  /// Terminates the stream with a user-provided sink and returns a [Sink] ready to start.
  ///
  /// @param sink the terminal sink (must not be null)
  /// @return a [Sink] ready to start
  /// @throws NullPointerException if `sink` is null
  Sink<T> toCustom(final MessageSink<T> sink);

  /// Terminates the stream by fanning out to multiple sinks. Each delivered message is dispatched
  /// to every sink; an exception in one sink is **logged at ERROR but suppressed** so the
  /// remaining sinks still receive the message (see [org.kpipe.sink.CompositeMessageSink]).
  ///
  /// **Best-effort delivery, NOT at-least-once-per-sink.** Because exceptions are suppressed,
  /// a sink that consistently fails will silently drop messages while the consumer continues to
  /// mark them as processed and commit offsets. The consumer's error path / DLQ is not invoked
  /// for per-sink failures — only the per-sink ERROR log.
  ///
  /// If you need guaranteed delivery to every sink, do not use `toMulti`. Wire each sink as its
  /// own pipeline through the explicit [org.kpipe.registry.MessageProcessorRegistry] API and
  /// drive failure handling through the consumer's `errorHandler` / DLQ configuration.
  ///
  /// @param sinks the sinks to broadcast to (must contain at least one element)
  /// @return a [Sink] ready to start
  /// @throws IllegalArgumentException if `sinks` is empty
  /// @throws NullPointerException if `sinks` is null
  @SuppressWarnings("unchecked")
  Sink<T> toMulti(final MessageSink<T>... sinks);
}
