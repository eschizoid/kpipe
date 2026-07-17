package io.github.eschizoid.kpipe;

import io.github.eschizoid.kpipe.consumer.KPipeConsumer;
import io.github.eschizoid.kpipe.registry.MessagePipeline;
import io.github.eschizoid.kpipe.registry.MessageProcessorRegistry;
import io.github.eschizoid.kpipe.registry.Result;
import io.github.eschizoid.kpipe.sink.MessageSink;
import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

/// Package-private [Sink] impl. Holds the configured [DefaultStream] plus the chosen terminal
/// [MessageSink] and constructs a fully-wired [KPipeConsumer] on `start()`. The consumer hosts
/// its own lifecycle since the 1.13 KPipeRunner fold.
///
/// Single-shot: the [#started] guard makes a second [#start()] throw rather than silently spin up
/// a second live consumer (the underlying consumer cannot be restarted). [MultiBuilder] does not
/// call this sink's `start()` — it reads [#buildPipeline()] to fold the route into one shared
/// consumer — so each route-sink stays independently single-shot.
///
/// @param <T> the deserialized message type
final class DefaultSink<T> implements Sink<T> {
  private static final Logger LOGGER = System.getLogger(DefaultSink.class.getName());

  private final DefaultStream<T> stream;
  private final MessageSink<T> terminalSink;
  private final AtomicBoolean started = new AtomicBoolean(false);

  DefaultSink(final DefaultStream<T> stream, final MessageSink<T> terminalSink) {
    this.stream = Objects.requireNonNull(stream, "stream cannot be null");
    this.terminalSink = Objects.requireNonNull(terminalSink, "terminalSink cannot be null");
  }

  @Override
  public Handle start() {
    if (!started.compareAndSet(false, true)) throw new IllegalStateException(
      "Sink already started — a Sink is single-shot; build a fresh Stream/Sink to run another consumer"
    );
    final var consumerBuilder = KPipeConsumer.builder()
      .withProperties(stream.kafkaProps())
      .withTopics(stream.topics())
      .withPipeline(buildPipeline())
      .withProcessingMode(stream.processingMode())
      .withKeyOrderedMaxKeys(stream.keyOrderedMaxKeys());

    stream.applyCommonConsumerConfig(consumerBuilder);

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
    return new MessagePipeline<>() {
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
          case Result.Passed<T> _ -> {
          }
          case Result.Filtered<T> _ -> {
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
      LOGGER.log(Level.WARNING, () -> name + " observer threw; swallowing to keep the pipeline running", e);
    }
  }

  /// Exposes the configured stream for test inspection of composition.
  DefaultStream<T> stream() {
    return stream;
  }

  /// Exposes the terminal sink for test inspection of composition (verifies
  /// that `toConsole()` / `toCustom()` / `toMulti()` produced the expected sink type).
  MessageSink<T> terminalSink() {
    return terminalSink;
  }
}
