package org.kpipe;

import java.util.Objects;
import org.kpipe.consumer.KPipeConsumer;
import org.kpipe.consumer.KPipeRunner;
import org.kpipe.registry.MessagePipeline;
import org.kpipe.registry.MessageProcessorRegistry;
import org.kpipe.sink.BatchPolicy;
import org.kpipe.sink.PartialBatchSink;

/// Package-private [Sink] impl for partial-batch terminals. Builds a sink-less
/// [org.kpipe.registry.MessagePipeline] from the stream's operator chain
/// (deserialize → operators → return value) and wires it into
/// [KPipeConsumer.Builder#withPartialBatchPipeline] together with the configured
/// [PartialBatchSink] and [BatchPolicy]. Honors whatever processing mode the underlying
/// [DefaultStream] carries (default: parallel); consumer-level config (metrics, errorHandler,
/// DLQ, pollTimeout, retry, backpressure) carries over too.
///
/// `Stream.toBatchPartial(...)` produces a single-topic instance. [MultiBuilder] also constructs
/// single-topic instances per route and collects them at `start()` time into one consumer
/// subscribing to multiple topics.
///
/// @param <T> the deserialized value type
record DefaultPartialBatchSink<T>(
  DefaultStream<T> stream,
  PartialBatchSink<T> partialSink,
  BatchPolicy batchPolicy
) implements Sink<T> {
  DefaultPartialBatchSink {
    Objects.requireNonNull(stream, "stream cannot be null");
    Objects.requireNonNull(partialSink, "partialSink cannot be null");
    Objects.requireNonNull(batchPolicy, "batchPolicy cannot be null");
    if (stream.topics().size() != 1) throw new IllegalArgumentException(
      "DefaultPartialBatchSink expects a single-topic stream; got %d topics".formatted(stream.topics().size())
    );
  }

  /// The topic this batch route is bound to. Single-topic by construction; multi-topic
  /// consumers compose multiple `DefaultPartialBatchSink` instances via [MultiBuilder].
  String topic() {
    return stream.topics().iterator().next();
  }

  /// Builds the typed pipeline (deserialize → operators → return value) used by the consumer to
  /// turn each `byte[]` into a `T` before enqueueing into the batch buffer.
  MessagePipeline<T> buildPipeline() {
    final var registry = new MessageProcessorRegistry(stream.format());
    final var pipelineBuilder = registry.pipeline(stream.format()).skipBytes(stream.skipBytes());
    for (final var op : stream.operators()) pipelineBuilder.add(op);
    return pipelineBuilder.build();
  }

  @Override
  public Handle start() {
    final var consumerBuilder = KPipeConsumer.<byte[]>builder()
      .withProperties(stream.kafkaProps())
      .withSequentialProcessing(stream.sequentialProcessing())
      .withPartialBatchPipeline(topic(), buildPipeline(), partialSink, batchPolicy);

    if (stream.maxRetries() > 0) consumerBuilder.withRetry(stream.maxRetries(), stream.retryBackoff());
    if (stream.backpressureHigh() != null) consumerBuilder.withBackpressure(
      stream.backpressureHigh(),
      stream.backpressureLow()
    );
    if (stream.consumerMetrics() != null) consumerBuilder.withMetrics(stream.consumerMetrics());
    if (stream.errorHandler() != null) consumerBuilder.withErrorHandler(stream.errorHandler()::accept);
    if (stream.deadLetterTopic() != null) consumerBuilder.withDeadLetterTopic(stream.deadLetterTopic());
    if (stream.pollTimeout() != null) consumerBuilder.withPollTimeout(stream.pollTimeout());

    final var consumer = consumerBuilder.build();
    final var runner = KPipeRunner.builder(consumer).build();
    try {
      runner.start();
    } catch (final RuntimeException e) {
      try {
        consumer.close();
      } catch (final Exception suppressed) {
        e.addSuppressed(suppressed);
      }
      throw e;
    }
    return new DefaultHandle(runner, consumer);
  }
}
