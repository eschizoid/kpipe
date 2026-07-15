package io.github.eschizoid.kpipe;

import io.github.eschizoid.kpipe.consumer.KPipeConsumer;
import io.github.eschizoid.kpipe.registry.MessagePipeline;
import io.github.eschizoid.kpipe.registry.MessageProcessorRegistry;
import io.github.eschizoid.kpipe.sink.BatchPolicy;
import io.github.eschizoid.kpipe.sink.BatchSink;
import java.util.Objects;

/// Package-private [Sink] impl for batch terminals. Builds a sink-less
/// [io.github.eschizoid.kpipe.registry.MessagePipeline]
/// from the stream's operator chain (deserialize → operators → return value) and wires it into
/// [KPipeConsumer.Builder#withBatchPipeline] together with the configured [BatchSink] and
/// [BatchPolicy]. Honors whatever processing mode the underlying [DefaultStream] carries
/// (default: parallel); consumer-level config (metrics, errorHandler, DLQ, pollTimeout, retry,
/// backpressure, tracer, circuit breaker) carries over too via
/// [DefaultStream#applyCommonConsumerConfig].
///
/// `Stream.toBatch(...)` produces a single-topic instance. [MultiBuilder] also constructs single-
/// topic instances per route and collects them at `start()` time into one consumer subscribing
/// to multiple topics.
///
/// @param <T> the deserialized value type
record DefaultBatchSink<T>(
  DefaultStream<T> stream,
  BatchSink<T> batchSink,
  BatchPolicy batchPolicy
) implements Sink<T> {
  DefaultBatchSink {
    Objects.requireNonNull(stream, "stream cannot be null");
    Objects.requireNonNull(batchSink, "batchSink cannot be null");
    Objects.requireNonNull(batchPolicy, "batchPolicy cannot be null");
    if (stream.topics().size() != 1) throw new IllegalArgumentException(
      "DefaultBatchSink expects a single-topic stream; got %d topics".formatted(stream.topics().size())
    );
  }

  /// The topic this batch route is bound to. Single-topic by construction; multi-topic
  /// consumers compose multiple `DefaultBatchSink` instances via [MultiBuilder].
  String topic() {
    return stream.topics().iterator().next();
  }

  /// Builds the typed pipeline (deserialize → operators → return value) used by the consumer to
  /// turn each `byte[]` into a `T` before enqueueing into the batch buffer.
  MessagePipeline<T> buildPipeline() {
    final var registry = new MessageProcessorRegistry();
    final var pipelineBuilder = registry.pipeline(stream.format()).skipBytes(stream.skipBytes());
    for (final var op : stream.operators()) pipelineBuilder.add(op);
    return pipelineBuilder.build();
  }

  @Override
  public Handle start() {
    final var consumerBuilder = KPipeConsumer.builder()
      .withProperties(stream.kafkaProps())
      .withProcessingMode(stream.processingMode())
      .withKeyOrderedMaxKeys(stream.keyOrderedMaxKeys())
      .withBatchPipeline(topic(), buildPipeline(), batchSink, batchPolicy);

    stream.applyCommonConsumerConfig(consumerBuilder);

    return DefaultHandle.startAndWrap(consumerBuilder.build());
  }
}
