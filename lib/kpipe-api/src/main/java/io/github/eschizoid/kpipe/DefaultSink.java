package io.github.eschizoid.kpipe;

import io.github.eschizoid.kpipe.consumer.KPipeConsumer;
import io.github.eschizoid.kpipe.registry.MessagePipeline;
import io.github.eschizoid.kpipe.registry.MessageProcessorRegistry;
import io.github.eschizoid.kpipe.registry.Result;
import io.github.eschizoid.kpipe.sink.MessageSink;
import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.util.Objects;
import java.util.function.Consumer;

/// Package-private [Sink] impl. Holds the configured [DefaultStream] plus the chosen terminal
/// [MessageSink] and constructs a fully-wired [KPipeConsumer] on `start()`. The consumer hosts
/// its own lifecycle since the 1.13 KPipeRunner fold.
///
/// @param <T> the deserialized message type
record DefaultSink<T>(DefaultStream<T> stream, MessageSink<T> terminalSink) implements Sink<T> {
  private static final Logger LOGGER = System.getLogger(DefaultSink.class.getName());

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
      .withProcessingMode(stream.processingMode())
      .withKeyOrderedMaxKeys(stream.keyOrderedMaxKeys());

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

    return DefaultHandle.startAndWrap(consumerBuilder.build());
  }

  /// Builds the [MessagePipeline] for this sink's stream + terminal sink. Reused by
  /// [MultiBuilder] when assembling a per-topic pipeline map. Wraps the built pipeline with
  /// observer dispatch when any of `onFiltered` / `onFailed` / `peekResult` is configured.
  MessagePipeline<?> buildPipeline() {
    final var registry = new MessageProcessorRegistry();
    final var pipelineBuilder = registry.pipeline(stream.format()).skipBytes(stream.skipBytes());
    for (final var op : stream.operators()) pipelineBuilder.add(op);
    final var base = pipelineBuilder.toSink(terminalSink).build();
    return wrapWithObservers(base);
  }

  private MessagePipeline<T> wrapWithObservers(final MessagePipeline<T> base) {
    final var onFiltered = stream.onFilteredObserver();
    final var onFailed = stream.onFailedObserver();
    final var peek = stream.peekResultObserver();
    if (onFiltered == null && onFailed == null && peek == null) return base;
    return new MessagePipeline<T>() {
      @Override
      public T deserialize(final byte[] data) {
        return base.deserialize(data);
      }

      @Override
      public byte[] serialize(final T data) {
        return base.serialize(data);
      }

      @Override
      public Result<T> process(final T data) {
        final var result = base.process(data);
        if (peek != null) safeAccept(peek, result, "peekResult");
        switch (result) {
          case Result.Passed<T> __ -> {
          }
          case Result.Filtered<T> __ -> {
            if (onFiltered != null) safeRun(onFiltered);
          }
          case Result.Failed<T> failed -> {
            if (onFailed != null) safeAccept(onFailed, failed.cause(), "onFailed");
          }
        }
        return result;
      }

      @Override
      public MessageSink<T> getSink() {
        return base.getSink();
      }
    };
  }

  private static void safeRun(final Runnable observer) {
    try {
      observer.run();
    } catch (final RuntimeException e) {
      LOGGER.log(Level.WARNING, "onFiltered observer threw; swallowing to keep the pipeline running", e);
    }
  }

  private static <A> void safeAccept(final Consumer<A> observer, final A arg, final String name) {
    try {
      observer.accept(arg);
    } catch (final RuntimeException e) {
      LOGGER.log(Level.WARNING, name + " observer threw; swallowing to keep the pipeline running", e);
    }
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
