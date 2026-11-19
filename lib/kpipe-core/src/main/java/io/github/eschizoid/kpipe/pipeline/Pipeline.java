package io.github.eschizoid.kpipe.pipeline;

import io.github.eschizoid.kpipe.record.RecordContext;
import io.github.eschizoid.kpipe.sink.CompositeMessageSink;
import io.github.eschizoid.kpipe.sink.MessageSink;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;

/// A unified pipeline interface for the lifecycle:
/// byte[] (Kafka) -> T (deserialized) -> T (processed) -> sink.
///
/// ## Error contract
///
/// `process(T)` returns a [Result] that distinguishes three outcomes at the type level:
/// - **`Passed<T>`** — non-null processed value flows to the sink.
/// - **`Filtered<T>`** — intentional drop. The caller treats the message as handled (e.g. commits
///   the offset) but skips downstream sinks.
/// - **`Failed<T>`** — non-fatal exception captured. The caller routes the cause to retry / DLQ /
///   error-handler logic.
///
/// `deserialize(byte[])` throws [MalformedRecordException] on bytes it cannot decode. `Result` is
/// reserved for the `process()` boundary, where the filter-vs-fail distinction lives. Callers do
/// `deserializeOrFail(bytes)`, then `switch (process(value))`, and handle the three `Result` cases
/// per their own semantics.
///
/// @param <T> the type of the object in the pipeline.
public interface Pipeline<T> {
  /// Starts building a pipeline that reads values with `format`.
  ///
  /// ```java
  /// final var pipeline = Pipeline.from(JsonFormat.INSTANCE)
  ///   .pipe(JsonFields.remove("password"))
  ///   .toSink(sink)
  ///   .build();
  /// ```
  ///
  /// @param format the format that turns record bytes into values and back
  /// @param <T>    the type of the value the pipeline carries
  /// @return a new builder
  static <T> Builder<T> from(final MessageFormat<T> format) {
    return new Builder<>(format);
  }

  /// Deserializes the raw byte array into a typed object.
  ///
  /// Implementations must throw rather than return `null`: [MalformedRecordException] for bytes
  /// that cannot be decoded, including a null or empty value, and any other exception for a
  /// failure a retry may fix, such as a schema registry that is down.
  ///
  /// @param data the raw data from Kafka
  /// @return the deserialized object (never null)
  T deserialize(byte[] data);

  /// Applies the chain of transformations to the typed object.
  ///
  /// Returns a [Result] distinguishing Passed / Filtered / Failed at the type level. The
  /// implementation may throw fatal exceptions (`Error`, `InterruptedException` etc.) — only
  /// non-fatal `RuntimeException`s should be captured into `Failed`.
  ///
  /// @param data the deserialized object
  /// @return a `Result<T>` describing the outcome
  Result<T> process(T data);

  /// Returns the terminal sink configured for this pipeline, if any.
  ///
  /// @return the message sink, or null if none is configured
  default MessageSink<T> getSink() {
    return null;
  }

  /// Calls [#deserialize] and throws [MalformedRecordException] when it returns null, which a
  /// pipeline must not do. A pipeline built by [Builder] never returns null; this
  /// guards a hand-written one.
  ///
  /// @param data the raw data from Kafka
  /// @return the deserialized object (never null)
  /// @throws MalformedRecordException if `deserialize` returns null
  default T deserializeOrFail(byte[] data) {
    final var deserialized = deserialize(data);
    if (deserialized == null) throw new MalformedRecordException(
      data == null
        ? "The record value is null (a tombstone), so there is nothing to deserialize"
        : "deserialize() returned null for a " + data.length + "-byte value instead of throwing"
    );
    return deserialized;
  }

  /// Builds a [Pipeline]: a format, optional bytes to skip, operators applied in order, and
  /// at most one sink. Start one with [Pipeline#from(MessageFormat)].
  ///
  /// @param <T> the type of the value the pipeline carries
  final class Builder<T> {

    private final MessageFormat<T> format;
    private final List<UnaryOperator<T>> operators = new ArrayList<>();
    private MessageSink<T> sink;
    private int skipBytes = 0;

    Builder(final MessageFormat<T> format) {
      this.format = Objects.requireNonNull(format, "format cannot be null");
    }

    /// Configures the pipeline to skip a certain number of bytes before deserialization.
    ///
    /// Useful for wire formats that include magic bytes or schema IDs (e.g. Confluent Magic Bytes).
    ///
    /// @param skipBytes The number of bytes to skip.
    /// @return This builder.
    public Builder<T> skipBytes(final int skipBytes) {
      this.skipBytes = skipBytes;
      return this;
    }

    /// Appends an operator. An operator that returns `null` filters the record; one that throws
    /// fails it.
    ///
    /// @param operator the operator to append
    /// @return this builder
    public Builder<T> pipe(final UnaryOperator<T> operator) {
      operators.add(Objects.requireNonNull(operator, "operator cannot be null"));
      return this;
    }

    /// Keeps records that match `keep` and filters the rest.
    ///
    /// @param keep returns true for records to keep
    /// @return this builder
    public Builder<T> filter(final Predicate<T> keep) {
      return pipe(Operators.filter(Objects.requireNonNull(keep, "keep cannot be null")));
    }

    /// Runs `sideEffect` on each record and passes it on unchanged.
    ///
    /// @param sideEffect the action to run
    /// @return this builder
    public Builder<T> peek(final Consumer<T> sideEffect) {
      return pipe(Operators.peek(Objects.requireNonNull(sideEffect, "sideEffect cannot be null")));
    }

    /// Applies `ifTrue` to records that match `condition` and `ifFalse` to the rest.
    ///
    /// @param condition the predicate to evaluate
    /// @param ifTrue the operator for matching records
    /// @param ifFalse the operator for the rest
    /// @return this builder
    public Builder<T> when(
      final Predicate<T> condition,
      final UnaryOperator<T> ifTrue,
      final UnaryOperator<T> ifFalse
    ) {
      Objects.requireNonNull(condition, "condition cannot be null");
      Objects.requireNonNull(ifTrue, "ifTrue operator cannot be null");
      Objects.requireNonNull(ifFalse, "ifFalse operator cannot be null");

      return pipe(obj -> condition.test(obj) ? ifTrue.apply(obj) : ifFalse.apply(obj));
    }

    /// Replaces the record's key for the rest of the attempt; see [RecordContext#rekey].
    ///
    /// @param keyFn computes the new key from the value; null means no key
    /// @return this builder
    public Builder<T> rekey(final Function<? super T, byte[]> keyFn) {
      return pipe(RecordContext.rekey(keyFn));
    }

    /// Replaces the record's key with a UTF-8 string for the rest of the attempt; see
    /// [RecordContext#rekey].
    ///
    /// @param keyFn computes the new key from the value; null means no key
    /// @return this builder
    public Builder<T> rekeyString(final Function<? super T, String> keyFn) {
      return pipe(RecordContext.rekeyString(keyFn));
    }

    /// Adds a header for the rest of the attempt; see [RecordContext#withHeader].
    ///
    /// @param name    the header name
    /// @param valueFn computes the header value from the value
    /// @return this builder
    public Builder<T> withHeader(final String name, final Function<? super T, byte[]> valueFn) {
      return pipe(RecordContext.withHeader(name, valueFn));
    }

    /// Sets the pipeline's sink. A pipeline has one; to deliver to several, use [#toMulti].
    ///
    /// A sink that throws fails the record, which goes through the configured retries, then the
    /// error handler and DLQ. To deliver to several systems and fail the record when any of them
    /// fails, write one sink that calls each in turn.
    ///
    /// @param sink the sink
    /// @return this builder
    /// @throws IllegalStateException if the pipeline already has a sink
    public Builder<T> toSink(final MessageSink<T> sink) {
      Objects.requireNonNull(sink, "sink cannot be null");
      if (this.sink != null) throw new IllegalStateException(
        "This pipeline already has a sink; use toMulti to deliver to several"
      );
      this.sink = sink;
      return this;
    }

    /// Delivers each record to every one of `sinks`, in order, as `Stream.toMulti` does.
    /// Delivery is best effort: a sink that throws is logged and the others still run, and the
    /// record does not fail.
    ///
    /// @param sinks the sinks, at least one
    /// @return this builder
    /// @throws IllegalArgumentException if no sink is given
    /// @throws IllegalStateException if the pipeline already has a sink
    @SafeVarargs
    public final Builder<T> toMulti(final MessageSink<T>... sinks) {
      if (sinks.length == 0) throw new IllegalArgumentException("toMulti needs at least one sink");
      return toSink(new CompositeMessageSink<>(List.of(sinks)));
    }

    /// Builds the [Pipeline].
    ///
    /// @return A new Pipeline instance.
    public Pipeline<T> build() {
      final var pipelineOperators = List.copyOf(operators);
      final var pipelineSink = this.sink;
      final var bytesToSkip = this.skipBytes;
      // getSimpleName() walks the class hierarchy; compute it once at build time, not per record.
      final var formatName = format.getClass().getSimpleName();
      final var registryBacked = format.isSchemaRegistryBacked();

      return new Pipeline<>() {
        @Override
        public MessageSink<T> getSink() {
          return pipelineSink;
        }

        @Override
        public T deserialize(byte[] data) {
          if (data == null) throw new MalformedRecordException(
            "The record value is null (a tombstone), so there is nothing to deserialize"
          );
          if (bytesToSkip > 0 && data.length <= bytesToSkip) throw new MalformedRecordException(
            "The record value is " +
              data.length +
              " bytes, so skipBytes(" +
              bytesToSkip +
              ") leaves nothing to deserialize"
          );
          final T value;
          if (bytesToSkip > 0) {
            final var actualData = new byte[data.length - bytesToSkip];
            System.arraycopy(data, bytesToSkip, actualData, 0, actualData.length);
            try {
              value = format.deserialize(actualData);
            } catch (final RuntimeException e) {
              throw PipelineDiagnostics.wrap(data, bytesToSkip, formatName, registryBacked, e);
            }
          } else {
            try {
              value = format.deserialize(data);
            } catch (final RuntimeException e) {
              throw PipelineDiagnostics.wrap(data, 0, formatName, registryBacked, e);
            }
          }
          // An empty value is the format's call: raw bytes pass it through, the decoding formats
          // return null for it.
          if (value == null) throw new MalformedRecordException(
            data.length == 0
              ? "The record value is empty, so there is nothing to deserialize"
              : formatName + " returned null for a " + data.length + "-byte value instead of throwing"
          );
          return value;
        }

        @Override
        public Result<T> process(T data) {
          if (data == null) return Result.filtered();
          var current = data;
          for (final var operator : pipelineOperators) {
            try {
              current = operator.apply(current);
            } catch (final RuntimeException e) {
              return Result.failed(e);
            }
            if (current == null) return Result.filtered();
          }
          return Result.passed(current);
        }
      };
    }
  }
}
