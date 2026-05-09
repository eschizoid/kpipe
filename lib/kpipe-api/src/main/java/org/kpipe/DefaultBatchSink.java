package org.kpipe;

import java.util.Objects;
import org.kpipe.consumer.KPipeConsumer;
import org.kpipe.consumer.KPipeRunner;
import org.kpipe.registry.MessageProcessorRegistry;
import org.kpipe.sink.BatchPolicy;
import org.kpipe.sink.BatchSink;

/// Package-private [Sink] impl for batch terminals. Builds a sink-less [org.kpipe.registry.MessagePipeline]
/// from the stream's operator chain (deserialize → operators → return value) and wires it into
/// [KPipeConsumer.Builder#withBatchPipeline] together with the configured [BatchSink] and
/// [BatchPolicy]. Honors whatever processing mode the underlying [DefaultStream] carries
/// (default: parallel); consumer-level config (metrics, errorHandler, DLQ, pollTimeout, retry,
/// backpressure) carries over too.
///
/// Single-topic only — the stream is rejected at construction time if it carries more than one
/// topic. Multi-topic batching ships in a follow-up.
///
/// @param <T> the deserialized value type
record DefaultBatchSink<T>(DefaultStream<T> stream, BatchSink<T> batchSink, BatchPolicy batchPolicy)
  implements Sink<T> {
  DefaultBatchSink {
    Objects.requireNonNull(stream, "stream cannot be null");
    Objects.requireNonNull(batchSink, "batchSink cannot be null");
    Objects.requireNonNull(batchPolicy, "batchPolicy cannot be null");
    if (stream.topics().size() != 1) throw new IllegalArgumentException(
      "toBatch is single-topic only in v1; got %d topics".formatted(stream.topics().size())
    );
  }

  @Override
  public Handle start() {
    final var topic = stream.topics().iterator().next();
    final var registry = new MessageProcessorRegistry(stream.format());
    final var pipelineBuilder = registry.pipeline(stream.format()).skipBytes(stream.skipBytes());
    for (final var op : stream.operators()) pipelineBuilder.add(op);
    final var pipeline = pipelineBuilder.build();

    final var consumerBuilder = KPipeConsumer.<byte[]>builder()
      .withProperties(stream.kafkaProps())
      .withSequentialProcessing(stream.sequentialProcessing())
      .withBatchPipeline(topic, pipeline, batchSink, batchPolicy);

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
