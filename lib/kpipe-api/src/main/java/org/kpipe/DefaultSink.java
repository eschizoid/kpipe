package org.kpipe;

import java.util.Objects;
import org.kpipe.consumer.KPipeConsumer;
import org.kpipe.registry.MessagePipeline;
import org.kpipe.registry.MessageProcessorRegistry;
import org.kpipe.sink.MessageSink;

/// Package-private [Sink] impl. Holds the configured [DefaultStream] plus the chosen terminal
/// [MessageSink] and constructs a fully-wired [KPipeConsumer] on `start()`. The consumer hosts
/// its own lifecycle since the 1.13 KPipeRunner fold.
///
/// @param <T> the deserialized message type
record DefaultSink<T>(DefaultStream<T> stream, MessageSink<T> terminalSink) implements Sink<T> {
  DefaultSink(final DefaultStream<T> stream, final MessageSink<T> terminalSink) {
    this.stream = Objects.requireNonNull(stream, "stream cannot be null");
    this.terminalSink = Objects.requireNonNull(terminalSink, "terminalSink cannot be null");
  }

  @Override
  public Handle start() {
    final var consumerBuilder = KPipeConsumer.<byte[]>builder()
      .withProperties(stream.kafkaProps())
      .withTopics(stream.topics())
      .withPipeline(buildPipeline())
      .withSequentialProcessing(stream.sequentialProcessing());

    if (stream.maxRetries() > 0) consumerBuilder.withRetry(stream.maxRetries(), stream.retryBackoff());
    if (stream.backpressureHigh() != null) consumerBuilder.withBackpressure(
      stream.backpressureHigh(),
      stream.backpressureLow()
    );
    if (stream.consumerMetrics() != null) consumerBuilder.withMetrics(stream.consumerMetrics());
    if (stream.errorHandler() != null) consumerBuilder.withErrorHandler(stream.errorHandler()::accept);
    if (stream.deadLetterTopic() != null) consumerBuilder.withDeadLetterTopic(stream.deadLetterTopic());
    if (stream.pollTimeout() != null) consumerBuilder.withPollTimeout(stream.pollTimeout());
    if (stream.tracer() != null) consumerBuilder.withTracer(stream.tracer());
    if (stream.circuitBreaker() != null) consumerBuilder.withCircuitBreaker(stream.circuitBreaker());

    final var consumer = consumerBuilder.build();
    try {
      consumer.start();
    } catch (final RuntimeException e) {
      // Releasing the consumer here keeps the AutoCloseable contract reachable when start() throws
      // before a Handle is returned.
      try {
        consumer.close();
      } catch (final Exception suppressed) {
        e.addSuppressed(suppressed);
      }
      throw e;
    }
    return new DefaultHandle(consumer);
  }

  /// Builds the [MessagePipeline] for this sink's stream + terminal sink. Reused by
  /// [MultiBuilder] when assembling a per-topic pipeline map.
  MessagePipeline<?> buildPipeline() {
    final var registry = new MessageProcessorRegistry(stream.format());
    final var pipelineBuilder = registry.pipeline(stream.format()).skipBytes(stream.skipBytes());
    for (final var op : stream.operators()) pipelineBuilder.add(op);
    return pipelineBuilder.toSink(terminalSink).build();
  }

  /// Exposes the configured stream for test inspection of composition.
  @Override
  public DefaultStream<T> stream() {
    return stream;
  }

  /// Exposes the terminal sink for test inspection of composition (verifies
  /// that `toConsole()` / `toCustom()` / `toMulti()` produced the expected sink type).
  @Override
  public MessageSink<T> terminalSink() {
    return terminalSink;
  }
}
