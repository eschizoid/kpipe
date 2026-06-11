package io.github.eschizoid.kpipe.registry;

import java.util.Objects;

/// Three-way outcome of a [MessagePipeline] stage. Replaces the pre-1.13.0 convention (`null =
/// filter`, `throw = fail`) with a type-level distinction enforced by the compiler.
///
/// Use the static factories or pattern-match exhaustively:
///
/// ```java
/// switch (pipeline.process(record)) {
///   case Result.Passed<T> p   -> sink.accept(p.value());
///   case Result.Filtered<T> _ -> markOffsetProcessed(record);
///   case Result.Failed<T> f   -> handleProcessingError(record, f.cause());
/// }
/// ```
///
/// **Why this exists.** Earlier pipelines used "null = filter, throw = fail" as a convention
/// enforced by discipline. A protobuf decode bug (a wrong `skipBytes(5)`) silently turned
/// deserialization failures into intentional filters, masked by an inflated processed counter.
/// `Result<T>` upgrades the convention to a compile-time guarantee: `Passed`, `Filtered`, and
/// `Failed` are distinct types, and pattern-matching is exhaustive — the compiler refuses to
/// let a caller confuse "intentional drop" with "swallowed error."
///
/// **Allocation note.** `Passed` and `Failed` allocate one record per call (on the hot path that's
/// one per record per pipeline stage). `Filtered` returns a shared singleton via [#filtered()] to
/// avoid an allocation in the common filter case. JMH-validate before claiming "zero-cost" — the
/// `Passed` allocation is the real cost.
///
/// @param <T> the pipeline value type
public sealed interface Result<T> permits Result.Passed, Result.Filtered, Result.Failed {
  /// The record passed through the stage successfully.
  ///
  /// @param value the processed value (must be non-null — use [#filtered()] for intentional
  ///     drops)
  /// @param <T>   the pipeline value type
  record Passed<T>(T value) implements Result<T> {
    /// Compact constructor enforcing the non-null invariant.
    public Passed {
      Objects.requireNonNull(value, "Passed.value cannot be null; use Filtered for intentional drops");
    }
  }

  /// The record was intentionally filtered out by the pipeline. Carries no value because there is
  /// no value to carry — the consumer should mark the offset as processed without invoking the
  /// sink.
  ///
  /// Use [#filtered()] to obtain the shared singleton instead of `new Filtered<>()` on the hot
  /// path.
  ///
  /// @param <T> the pipeline value type
  record Filtered<T>() implements Result<T> {
    private static final Filtered<?> SHARED = new Filtered<>();

    /// Returns the shared `Filtered` sentinel. Avoids per-record allocation on filter-heavy
    /// pipelines. Safe to share across types since the record carries no state.
    ///
    /// @param <T> the pipeline value type
    /// @return the shared singleton, cast to `Filtered<T>`
    @SuppressWarnings("unchecked")
    public static <T> Filtered<T> instance() {
      return (Filtered<T>) SHARED;
    }
  }

  /// The pipeline failed processing the record. Carries the non-fatal exception that caused the
  /// failure — the consumer routes this to retry / DLQ / error-handler logic.
  ///
  /// @param cause the exception thrown by an operator or sink (must be non-null)
  /// @param <T>   the pipeline value type
  record Failed<T>(Throwable cause) implements Result<T> {
    /// Compact constructor enforcing the non-null invariant.
    public Failed {
      Objects.requireNonNull(cause, "Failed.cause cannot be null");
    }
  }

  /// Factory for the success case.
  ///
  /// @param value the processed value (must be non-null)
  /// @param <T>   the pipeline value type
  /// @return a new `Passed<T>` wrapping `value`
  static <T> Result<T> passed(final T value) {
    return new Passed<>(value);
  }

  /// Factory for the intentional-filter case. Returns the shared singleton.
  ///
  /// @param <T> the pipeline value type
  /// @return the shared `Filtered<T>` singleton
  static <T> Result<T> filtered() {
    return Filtered.instance();
  }

  /// Factory for the failure case.
  ///
  /// @param cause the exception that caused the failure (must be non-null)
  /// @param <T>   the pipeline value type
  /// @return a new `Failed<T>` wrapping `cause`
  static <T> Result<T> failed(final Throwable cause) {
    return new Failed<>(cause);
  }
}
