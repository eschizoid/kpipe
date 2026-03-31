package org.kpipe.registry;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;
import org.kpipe.sink.MessageSink;

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
  public TypedPipelineBuilder(MessageFormat<T> format, MessageProcessorRegistry registry) {
    this.format = Objects.requireNonNull(format, "format cannot be null");
    this.registry = Objects.requireNonNull(registry, "registry cannot be null");
  }

  /// Configures the pipeline to skip a certain number of bytes before deserialization.
  ///
  /// Useful for wire formats that include magic bytes or schema IDs (e.g. Confluent Magic Bytes).
  ///
  /// @param skipBytes The number of bytes to skip.
  /// @return This builder.
  public TypedPipelineBuilder<T> skipBytes(int skipBytes) {
    this.skipBytes = skipBytes;
    return this;
  }

  /// Adds a transformation operator to the pipeline.
  ///
  /// @param operator The operator to add.
  /// @return This builder.
  public TypedPipelineBuilder<T> add(final UnaryOperator<T> operator) {
    operators.add(Objects.requireNonNull(operator, "operator cannot be null"));
    return this;
  }

  /// Adds transformation operators from the registry.
  ///
  /// @param keys The registry keys for the operators.
  /// @return This builder.
  @SafeVarargs
  public final TypedPipelineBuilder<T> add(final RegistryKey<T>... keys) {
    for (final var key : keys) add(registry.getOperator(key));
    return this;
  }

  /// Adds a conditional operator to the pipeline.
  ///
  /// @param condition The predicate to evaluate.
  /// @param ifTrue The operator to apply if the condition is true.
  /// @param ifFalse The operator to apply if the condition is false.
  /// @return This builder.
  public TypedPipelineBuilder<T> when(
    final Predicate<T> condition,
    final UnaryOperator<T> ifTrue,
    final UnaryOperator<T> ifFalse
  ) {
    Objects.requireNonNull(condition, "condition cannot be null");
    Objects.requireNonNull(ifTrue, "ifTrue operator cannot be null");
    Objects.requireNonNull(ifFalse, "ifFalse operator cannot be null");

    return add(obj -> condition.test(obj) ? ifTrue.apply(obj) : ifFalse.apply(obj));
  }

  /// Sets a terminal sink for the pipeline.
  ///
  /// @param sink The sink to add.
  /// @return This builder.
  public TypedPipelineBuilder<T> toSink(final MessageSink<T> sink) {
    if (this.sink == null) {
      this.sink = Objects.requireNonNull(sink, "sink cannot be null");
    } else {
      final var currentSink = this.sink;
      this.sink = value -> {
        currentSink.accept(value);
        sink.accept(value);
      };
    }
    return this;
  }

  /// Sets terminal sinks for the pipeline from the registry.
  ///
  /// @param sinkKeys The registry keys for the sinks.
  /// @return This builder.
  @SafeVarargs
  public final TypedPipelineBuilder<T> toSink(RegistryKey<T>... sinkKeys) {
    for (final var key : sinkKeys) toSink(registry.sinkRegistry().get(key));
    return this;
  }

  /// Builds the [MessagePipeline].
  ///
  /// @return A new MessagePipeline instance.
  public MessagePipeline<T> build() {
    final var pipelineOperators = List.copyOf(operators);
    final var pipelineSink = this.sink;
    final var bytesToSkip = this.skipBytes;

    return new MessagePipeline<T>() {
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
          return format.deserialize(actualData);
        }
        return format.deserialize(data);
      }

      @Override
      public byte[] serialize(T data) {
        if (data == null) return null;
        return format.serialize(data);
      }

      @Override
      public T process(T data) {
        if (data == null) return null;
        var current = data;
        for (final var operator : pipelineOperators) {
          current = operator.apply(current);
          if (current == null) return null;
        }
        return current;
      }
    };
  }
}
