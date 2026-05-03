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
/// All builder methods are immutable in spirit — implementations may either return `this` or a
/// new instance, but callers should treat the chain as a fluent pipeline:
/// ```java
/// KPipe.json("events", kafkaProps)
///     .pipe(msg -> { msg.put("ts", System.currentTimeMillis()); return msg; })
///     .filter(msg -> "active".equals(msg.get("status")))
///     .toConsole()
///     .start();
/// ```
///
/// @param <T> the deserialized message type flowing through the pipeline
/// @since 1.11.0
public interface Stream<T> {
  /// Appends an operator to the pipeline.
  ///
  /// @param op the operator to append (must not be null)
  /// @return this stream (or a new instance with `op` appended)
  Stream<T> pipe(UnaryOperator<T> op);

  /// Drops messages where `keep` returns false. Equivalent to
  /// `pipe(Operators.filter(keep))`.
  ///
  /// @param keep predicate; messages for which it returns true are passed downstream
  /// @return this stream
  Stream<T> filter(Predicate<T> keep);

  /// Runs a side-effect on each message and passes it through unchanged.
  ///
  /// @param sideEffect the side-effect to run
  /// @return this stream
  Stream<T> peek(Consumer<T> sideEffect);

  /// Conditional branch — applies `ifTrue` when `cond` is true, otherwise `ifFalse`.
  ///
  /// @param cond the condition predicate
  /// @param ifTrue operator applied when `cond.test(msg)` is true
  /// @param ifFalse operator applied when `cond.test(msg)` is false
  /// @return this stream
  Stream<T> when(Predicate<T> cond, UnaryOperator<T> ifTrue, UnaryOperator<T> ifFalse);

  /// Configures retry behavior for failed message processing.
  ///
  /// @param maxRetries maximum number of retry attempts (must be >= 0)
  /// @param backoff duration to wait between attempts
  /// @return this stream
  Stream<T> withRetry(int maxRetries, Duration backoff);

  /// Enables backpressure with default watermarks (high=10000, low=7000).
  ///
  /// @return this stream
  Stream<T> withBackpressure();

  /// Enables backpressure with explicit watermarks.
  ///
  /// @param high pause threshold
  /// @param low resume threshold (must be < high)
  /// @return this stream
  Stream<T> withBackpressure(long high, long low);

  /// Switches between sequential and parallel (virtual-thread) processing.
  ///
  /// @param sequential true for sequential processing, false for parallel (default)
  /// @return this stream
  Stream<T> withSequentialProcessing(boolean sequential);

  /// Terminates the stream with a format-appropriate console sink.
  ///
  /// @return a [Sink] ready to start
  Sink<T> toConsole();

  /// Terminates the stream with a user-provided sink.
  ///
  /// @param sink the sink to use
  /// @return a [Sink] ready to start
  Sink<T> toCustom(MessageSink<T> sink);

  /// Terminates the stream by fanning out to multiple sinks.
  ///
  /// @param sinks the sinks to broadcast to
  /// @return a [Sink] ready to start
  @SuppressWarnings("unchecked")
  Sink<T> toMulti(MessageSink<T>... sinks);
}
