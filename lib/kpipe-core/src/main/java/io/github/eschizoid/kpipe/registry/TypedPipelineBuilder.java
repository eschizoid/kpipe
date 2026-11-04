package io.github.eschizoid.kpipe.registry;

import io.github.eschizoid.kpipe.sink.CompositeMessageSink;
import io.github.eschizoid.kpipe.sink.MessageSink;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;

/// A generic builder for creating type-safe [MessagePipeline] instances.
///
/// @param <T> The type of the object in the pipeline.
public final class TypedPipelineBuilder<T> {

  private final MessageFormat<T> format;
  private final List<UnaryOperator<T>> operators = new ArrayList<>();
  private final MessageProcessorRegistry registry;
  private MessageSink<T> sink;
  private int skipBytes = 0;

  /// Creates a new TypedPipelineBuilder.
  ///
  /// @param format   The message format for serialization/deserialization.
  /// @param registry The registry for looking up operators and sinks.
  public TypedPipelineBuilder(final MessageFormat<T> format, MessageProcessorRegistry registry) {
    this.format = Objects.requireNonNull(format, "format cannot be null");
    this.registry = Objects.requireNonNull(registry, "registry cannot be null");
  }

  /// Configures the pipeline to skip a certain number of bytes before deserialization.
  ///
  /// Useful for wire formats that include magic bytes or schema IDs (e.g. Confluent Magic Bytes).
  ///
  /// @param skipBytes The number of bytes to skip.
  /// @return This builder.
  public TypedPipelineBuilder<T> skipBytes(final int skipBytes) {
    this.skipBytes = skipBytes;
    return this;
  }

  /// Appends an operator. An operator that returns `null` filters the record; one that throws
  /// fails it.
  ///
  /// @param operator the operator to append
  /// @return this builder
  public TypedPipelineBuilder<T> pipe(final UnaryOperator<T> operator) {
    operators.add(Objects.requireNonNull(operator, "operator cannot be null"));
    return this;
  }

  /// Appends operators from the registry. Each one looks its key up when it runs, so
  /// re-registering a key takes effect at once.
  ///
  /// @param keys the registry keys of the operators
  /// @return this builder
  /// @throws IllegalArgumentException if no operator is registered under one of the keys
  @SafeVarargs
  public final TypedPipelineBuilder<T> pipe(final RegistryKey<T>... keys) {
    for (final var key : keys)
      if (!registry.hasOperator(key)) throw new IllegalArgumentException(
        "No operator registered under key %s; register it before adding it to a pipeline".formatted(key)
      );
    for (final var key : keys) pipe(registry.getOperator(key));
    return this;
  }

  /// Keeps records that match `keep` and filters the rest.
  ///
  /// @param keep returns true for records to keep
  /// @return this builder
  public TypedPipelineBuilder<T> filter(final Predicate<T> keep) {
    return pipe(Operators.filter(Objects.requireNonNull(keep, "keep cannot be null")));
  }

  /// Runs `sideEffect` on each record and passes it on unchanged.
  ///
  /// @param sideEffect the action to run
  /// @return this builder
  public TypedPipelineBuilder<T> peek(final Consumer<T> sideEffect) {
    return pipe(Operators.peek(Objects.requireNonNull(sideEffect, "sideEffect cannot be null")));
  }

  /// Applies `ifTrue` to records that match `condition` and `ifFalse` to the rest.
  ///
  /// @param condition the predicate to evaluate
  /// @param ifTrue the operator for matching records
  /// @param ifFalse the operator for the rest
  /// @return this builder
  public TypedPipelineBuilder<T> when(
    final Predicate<T> condition,
    final UnaryOperator<T> ifTrue,
    final UnaryOperator<T> ifFalse
  ) {
    Objects.requireNonNull(condition, "condition cannot be null");
    Objects.requireNonNull(ifTrue, "ifTrue operator cannot be null");
    Objects.requireNonNull(ifFalse, "ifFalse operator cannot be null");

    return pipe(obj -> condition.test(obj) ? ifTrue.apply(obj) : ifFalse.apply(obj));
  }

  /// Sets the pipeline's sink. A pipeline has one; to deliver to several, use [#toMulti].
  ///
  /// A sink that throws fails the record, which is then retried. To deliver to several systems and
  /// retry the record when any of them fails, write one sink that calls each in turn.
  ///
  /// @param sink the sink
  /// @return this builder
  /// @throws IllegalStateException if the pipeline already has a sink
  public TypedPipelineBuilder<T> toSink(final MessageSink<T> sink) {
    Objects.requireNonNull(sink, "sink cannot be null");
    if (this.sink != null) throw new IllegalStateException(
      "This pipeline already has a sink; use toMulti to deliver to several"
    );
    this.sink = sink;
    return this;
  }

  /// Sets the pipeline's sink from the registry. It looks its key up when it runs, so
  /// re-registering the key takes effect at once.
  ///
  /// @param key the registry key of the sink
  /// @return this builder
  /// @throws IllegalArgumentException if no sink is registered under `key`
  /// @throws IllegalStateException if the pipeline already has a sink
  public TypedPipelineBuilder<T> toSink(final RegistryKey<T> key) {
    requireSink(key);
    return toSink(registry.getSink(key));
  }

  /// Delivers each record to every one of `sinks`, in order, as `Stream.toMulti` does. Delivery is
  /// best effort: a sink that throws is logged and the others still run, and the record does not
  /// fail.
  ///
  /// @param sinks the sinks, at least one
  /// @return this builder
  /// @throws IllegalStateException if the pipeline already has a sink
  @SafeVarargs
  public final TypedPipelineBuilder<T> toMulti(final MessageSink<T>... sinks) {
    return toSink(new CompositeMessageSink<>(List.of(sinks)));
  }

  /// Delivers each record to the registry sinks under `keys`, as [#toMulti(MessageSink...)] does.
  ///
  /// @param keys the registry keys of the sinks
  /// @return this builder
  /// @throws IllegalArgumentException if no sink is registered under one of the keys
  /// @throws IllegalStateException if the pipeline already has a sink
  @SafeVarargs
  public final TypedPipelineBuilder<T> toMulti(final RegistryKey<T>... keys) {
    for (final var key : keys) requireSink(key);
    final var sinks = new ArrayList<MessageSink<T>>();
    for (final var key : keys) sinks.add(registry.getSink(key));
    return toSink(new CompositeMessageSink<>(sinks));
  }

  private void requireSink(final RegistryKey<T> key) {
    if (!registry.hasSink(key)) throw new IllegalArgumentException(
      "No sink registered under key %s; register it before adding it to a pipeline".formatted(key)
    );
  }

  /// Builds the [MessagePipeline].
  ///
  /// @return A new MessagePipeline instance.
  public MessagePipeline<T> build() {
    final var pipelineOperators = List.copyOf(operators);
    final var pipelineSink = this.sink;
    final var bytesToSkip = this.skipBytes;
    // Hoisted once at build-time — getClass().getSimpleName() walks the class hierarchy and is only
    // used inside the catch arm of deserialize. Computing it per-record put the cost on every
    // successful deserialize as well.
    final var formatName = format.getClass().getSimpleName();

    return new MessagePipeline<>() {
      @Override
      public MessageSink<T> getSink() {
        return pipelineSink;
      }

      @Override
      public T deserialize(byte[] data) {
        if (data == null) return null;
        if (bytesToSkip > 0) {
          if (data.length <= bytesToSkip) return null;
          final var actualData = new byte[data.length - bytesToSkip];
          System.arraycopy(data, bytesToSkip, actualData, 0, actualData.length);
          try {
            return format.deserialize(actualData);
          } catch (final RuntimeException e) {
            throw PipelineDiagnostics.wrap(data, bytesToSkip, formatName, e);
          }
        }
        try {
          return format.deserialize(data);
        } catch (final RuntimeException e) {
          throw PipelineDiagnostics.wrap(data, 0, formatName, e);
        }
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
