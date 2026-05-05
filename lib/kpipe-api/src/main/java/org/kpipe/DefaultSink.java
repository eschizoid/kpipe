package org.kpipe;

import java.util.Objects;
import org.kpipe.consumer.KPipeConsumer;
import org.kpipe.consumer.KPipeRunner;
import org.kpipe.registry.MessageProcessorRegistry;
import org.kpipe.sink.MessageSink;

/// Package-private [Sink] impl. Holds the configured [DefaultStream] plus the chosen terminal
/// [MessageSink] and constructs a fully-wired `KPipeConsumer` + `KPipeRunner` on `start()`.
///
/// @param <T> the deserialized message type
record DefaultSink<T>(DefaultStream<T> stream, MessageSink<T> terminalSink) implements Sink<T> {
  DefaultSink(final DefaultStream<T> stream, final MessageSink<T> terminalSink) {
    this.stream = Objects.requireNonNull(stream, "stream cannot be null");
    this.terminalSink = Objects.requireNonNull(terminalSink, "terminalSink cannot be null");
  }

  @Override
  public Handle start() {
    final var registry = new MessageProcessorRegistry(stream.format());
    final var pipelineBuilder = registry.pipeline(stream.format());
    for (final var op : stream.operators()) pipelineBuilder.add(op);
    final var pipeline = pipelineBuilder.toSink(terminalSink).build();

    final var consumerBuilder = KPipeConsumer.<byte[]>builder()
      .withProperties(stream.kafkaProps())
      .withTopic(stream.topic())
      .withPipeline(pipeline)
      .withSequentialProcessing(stream.sequentialProcessing());

    if (stream.maxRetries() > 0) consumerBuilder.withRetry(stream.maxRetries(), stream.retryBackoff());
    if (stream.backpressureHigh() != null) consumerBuilder.withBackpressure(
      stream.backpressureHigh(),
      stream.backpressureLow()
    );

    final var consumer = consumerBuilder.build();
    final var runner = KPipeRunner.builder(consumer).build();
    runner.start();
    return new DefaultHandle(runner, consumer);
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
