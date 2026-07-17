package io.github.eschizoid.kpipe;

import io.github.eschizoid.kpipe.consumer.KPipeConsumer;
import io.github.eschizoid.kpipe.registry.MessagePipeline;
import io.github.eschizoid.kpipe.registry.MessageProcessorRegistry;
import io.github.eschizoid.kpipe.sink.MessageSink;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

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
  /// [MultiBuilder] when assembling a per-topic pipeline map. Observer dispatch (`onFiltered` /
  /// `onFailed` / `peekResult`) is wired by [DefaultStream#wrapWithObservers] — shared with
  /// [DefaultBatchSink] so the two sink types can't drift on it.
  MessagePipeline<?> buildPipeline() {
    final var registry = new MessageProcessorRegistry();
    final var pipelineBuilder = registry.pipeline(stream.format()).skipBytes(stream.skipBytes());
    for (final var op : stream.operators()) pipelineBuilder.add(op);
    final var base = pipelineBuilder.toSink(terminalSink).build();
    return stream.wrapWithObservers(base);
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
